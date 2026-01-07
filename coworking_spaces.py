import boto3
import uuid
import json
import urllib.parse
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('coworking-spaces')
users_table = dynamodb.Table('users')


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def add_coworking_space(event):
    body = json.loads(event['body'])

    # IDs e básicos do espaço
    space_id = body['spaceId']
    name = body['name']
    city = body.get('city')  # já vinha
    country = body.get('country', 'Brasil')  # default se não vier
    district = body.get('district')  # já vinha
    capacity = body.get('capacity')
    amenities = body.get('amenities', [])
    availability = body.get('availability', True)
    hoster = body['hoster']

    # Novos campos de informações básicas / contato
    email = body.get('email')
    cnpj = body.get('cnpj')
    ddd = body.get('ddd')
    numero_telefone = body.get('numeroTelefone')
    telefone_completo = body.get('telefoneCompleto')  # já chega normalizado (apenas dígitos)
    razao_social = body.get('razaoSocial')

    # Novos campos de endereço (além de city/district/country)
    street = body.get('street')
    number = body.get('number')
    complement = body.get('complement')
    state = body.get('state')

    # Campos do espaço
    categoria = body.get('categoria')
    subcategoria = body.get('subcategoria')
    descricao = body.get('descricao')
    regras = body.get('regras')
    dias_semana = body.get('diasSemana', [])
    hora_inicio = body.get('horaInicio')
    hora_fim = body.get('horaFim')

    # Preços: podem vir como string (ex: "100.50") — converter para float → Decimal
    try:
        preco_hora = float(body.get('precoHora', 0) or 0)
    except Exception:
        preco_hora = 0.0
    try:
        preco_dia = float(body.get('precoDia', 0) or 0)
    except Exception:
        preco_dia = 0.0

    item = {
        'spaceId': space_id,
        'name': name,
        # Endereço completo
        'street': street,
        'number': number,
        'complement': complement,
        'district': district,
        'city': city,
        'state': state,
        'country': country,
        # Contato / empresa
        'email': email,
        'cnpj': cnpj,
        'ddd': ddd,
        'numeroTelefone': numero_telefone,
        'telefoneCompleto': telefone_completo,
        'razaoSocial': razao_social,
        # Dados do espaço
        'capacity': capacity,
        'amenities': amenities,
        'availability': availability,
        'hoster': hoster,
        'categoria': categoria,
        'subcategoria': subcategoria,
        'descricao': descricao,
        'regras': regras,
        'diasSemana': dias_semana,
        'horaInicio': hora_inicio,
        'horaFim': hora_fim,
        'precoHora': Decimal(str(preco_hora)),
        'precoDia': Decimal(str(preco_dia)),
    }

    # Imagem (opcional)
    imagem_url = body.get('imagemUrl')
    if imagem_url:
        item['imagemUrl'] = imagem_url

    # Remove chaves com valor None para não poluir o item no DynamoDB
    item = {k: v for k, v in item.items() if v is not None}

    print("✅ Salvando item no DynamoDB:", json.dumps(item, default=decimal_default))
    table.put_item(Item=item)

    # Marca o usuário como hoster
    try:
        users_table.update_item(
            Key={'userId': hoster},
            UpdateExpression="set isHoster = :true_val",
            ExpressionAttributeValues={':true_val': True},
            ReturnValues="UPDATED_NEW"
        )
        print(f"✅ Usuário {hoster} marcado como hoster na tabela de usuários.")
    except Exception as e:
        print(f"⚠️ Erro ao atualizar usuário {hoster} como hoster:", str(e))
        
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Coworking space added successfully',
            'spaceId': space_id
        })
    }


def get_coworking_space(event):
    space_id = event['queryStringParameters']['spaceId']
    response = table.get_item(Key={'spaceId': space_id})
    item = response.get('Item')

    if not item:
        return {
            'statusCode': 404,
            'body': json.dumps({'message': 'Coworking space not found'})
        }

    return {
        'statusCode': 200,
        'body': json.dumps(item, default=decimal_default)
    }


def get_available_coworking_spaces(event):
    print("⚙️ Escaneando coworking-spaces...")

    try:
        response = table.scan(
            FilterExpression="availability = :val",
            ExpressionAttributeValues={":val": True}
        )

        raw_items = response.get('Items', [])
        print("🔍 Itens brutos encontrados:", len(raw_items))

        items = []
        for item in raw_items:
            if 'amenities' in item and isinstance(item['amenities'], list):
                try:
                    if all(isinstance(a, dict) and 'S' in a for a in item['amenities']):
                        item['amenities'] = [a['S'] for a in item['amenities']]
                    elif not all(isinstance(a, str) for a in item['amenities']):
                        item['amenities'] = []
                except Exception as e:
                    print("⚠️ Erro ao processar amenities:", e)
                    item['amenities'] = []

            try:
                item['precoHora'] = float(item.get('precoHora', 0) or 0)
                item['precoDia'] = float(item.get('precoDia', 0) or 0)
            except Exception as e:
                print("⚠️ Erro ao processar preços:", e)
                item['precoHora'] = 0
                item['precoDia'] = 0

            for campo in ['name', 'city', 'district']:
                if campo not in item:
                    item[campo] = ""

            items.append(item)

        print("📤 Itens após conversão:", json.dumps(items, indent=2, default=decimal_default))

        return {
            'statusCode': 200,
            'body': json.dumps(items, default=decimal_default)
        }

    except Exception as e:
        print("❌ ERRO INTERNO NA LAMBDA:", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({"message": "Internal Server Error"})
        }


def update_coworking_space(event):
    query = event.get('queryStringParameters') or {}
    space_id = urllib.parse.unquote(query.get('spaceId', '')).strip()
    print("🔧 spaceId recebido no update:", repr(space_id))

    body = json.loads(event['body'])
    update_expression = "set"
    expression_attribute_values = {}

    # Campos atualizáveis (adicionei os novos também)
    def add_update(field, alias):
        nonlocal update_expression, expression_attribute_values
        if field in body:
            update_expression += f" {field} = {alias},"
            expression_attribute_values[alias] = body[field]

    add_update('name', ':n')
    add_update('city', ':c')
    add_update('country', ':co')
    add_update('district', ':d')
    add_update('street', ':st')
    add_update('number', ':nu')
    add_update('complement', ':cp')
    add_update('state', ':uf')

    add_update('email', ':em')
    add_update('cnpj', ':cn')
    add_update('ddd', ':dd')
    add_update('numeroTelefone', ':nt')
    add_update('telefoneCompleto', ':tc')
    add_update('razaoSocial', ':rs')

    add_update('capacity', ':ca')
    add_update('amenities', ':a')
    add_update('availability', ':av')
    add_update('hoster', ':h')
    add_update('categoria', ':cat')
    add_update('subcategoria', ':sub')
    add_update('descricao', ':desc')
    add_update('regras', ':reg')
    add_update('diasSemana', ':ds')
    add_update('horaInicio', ':hi')
    add_update('horaFim', ':hf')

    if 'precoHora' in body:
        try:
            expression_attribute_values[':ph'] = Decimal(str(float(body['precoHora'] or 0)))
        except Exception:
            expression_attribute_values[':ph'] = Decimal('0')
        update_expression += " precoHora = :ph,"
    if 'precoDia' in body:
        try:
            expression_attribute_values[':pd'] = Decimal(str(float(body['precoDia'] or 0)))
        except Exception:
            expression_attribute_values[':pd'] = Decimal('0')
        update_expression += " precoDia = :pd,"

    if 'imagemUrl' in body:
        update_expression += " imagemUrl = :img,"
        expression_attribute_values[':img'] = body['imagemUrl']

    update_expression = update_expression.rstrip(',')

    if update_expression == "set":
        raise ValueError("Nenhum atributo fornecido para atualização")

    response = table.update_item(
        Key={'spaceId': space_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ReturnValues="UPDATED_NEW"
    )

    return {
        'statusCode': 200,
        'body': json.dumps(response['Attributes'], default=decimal_default)
    }


def update_coworking_space_full(event):
    # Log do evento original (cuidado em produção)
    try:
        print("🟡 Event (keys):", list(event.keys()))
        print("🟡 Event body (raw):", (event.get('body') or '')[:1000])  # limita tamanho
        print("🟡 QueryStringParameters:", event.get('queryStringParameters'))
    except Exception as e:
        print("⚠️ Falha ao logar event:", str(e))

    # Parse do body
    try:
        body = json.loads(event.get('body') or '{}')
    except Exception as e:
        print("❌ JSON inválido no body:", str(e))
        return {'statusCode': 400, 'body': json.dumps({'message': 'Body inválido (JSON)'})}

    # Aceitar 'spaceId' OU 'id'
    space_id = (body.get('spaceId') or body.get('id') or '').strip()
    source = 'body'
    if not space_id:
        query = event.get('queryStringParameters') or {}
        space_id = (query.get('spaceId') or '').strip()
        source = 'queryString'

    print(f"🔎 space_id resolvido: '{space_id}' (origem: {source})")

    if not space_id:
        print("❌ spaceId ausente")
        return {'statusCode': 400, 'body': json.dumps({'message': 'spaceId obrigatório'})}

    # Mapeamento de campos (logando o que chegou e o que será enviado)
    updates = {}

    def set_if_present(src_key, dst_key, transform=lambda v: v):
        v = body.get(src_key, None)
        if v is not None:
            try:
                tv = transform(v)
                updates[dst_key] = tv
                print(f"✅ Campo mapeado: {src_key} -> {dst_key} = {repr(tv)}")
            except Exception as e:
                print(f"⚠️ Falha ao transformar {src_key} -> {dst_key}: {repr(v)}; erro: {str(e)}")
        else:
            print(f"⏭️ Campo ausente (não mapeado): {src_key}")

    set_if_present('title', 'name')
    set_if_present('description', 'descricao')
    set_if_present('capacity', 'capacity')
    set_if_present('pricePerHour', 'precoHora', lambda v: Decimal(str(float(v or 0))))
    # Atenção: confirme a semântica de availability vs isEnabled (true/false)
    set_if_present('isEnabled', 'availability')
    set_if_present('autoApprove', 'autoApprove')
    set_if_present('facilityIDs', 'amenities')
    set_if_present('weekdays', 'diasSemana')
    set_if_present('minDurationMinutes', 'minDurationMinutes')
    set_if_present('bufferMinutes', 'bufferMinutes')

    if not updates:
        print("❌ Nenhum campo para atualizar (body vazio ou sem chaves esperadas)")
        return {'statusCode': 400, 'body': json.dumps({'message': 'Nenhum campo para atualizar'})}

    update_expression = "set " + ", ".join([f"{k} = :{k}" for k in updates.keys()])
    expression_attribute_values = {f":{k}": v for k, v in updates.items()}

    print("🛠️ UpdateExpression:", update_expression)
    # Para evitar logar valores sensíveis, só mostre as chaves e tipos:
    sanitized_values = {k: f"{type(v).__name__}({repr(v)[:60]})" for k, v in expression_attribute_values.items()}
    print("🛠️ ExpressionAttributeValues (sanitized):", sanitized_values)

    try:
        resp = table.update_item(
            Key={'spaceId': space_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW"
        )
        print("✅ DynamoDB update OK:", json.dumps(resp.get('Attributes', {}), default=decimal_default)[:1000])
        return {'statusCode': 200, 'body': json.dumps(resp['Attributes'], default=decimal_default)}
    except Exception as e:
        print("❌ DynamoDB update_item falhou:", str(e))
        return {'statusCode': 500, 'body': json.dumps({'message': 'Falha ao atualizar', 'error': str(e)})}



def delete_coworking_space(event):
    space_id = event['queryStringParameters']['spaceId']
    table.delete_item(Key={'spaceId': space_id})

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Coworking space deleted successfully',
            'spaceId': space_id
        })
    }

def get_user_by_id(event):
    path_params = event.get('pathParameters') or {}
    user_id = path_params.get('userId')

    # ✅ Alternativa extra para extrair o userId manualmente do path
    if not user_id:
        full_path = event.get("requestContext", {}).get("http", {}).get("path", "")
        if "/users/" in full_path:
            user_id = full_path.split("/users/")[-1].strip("/")

    if not user_id:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'userId obrigatório'})
        }

    try:
        response = users_table.get_item(Key={'userId': user_id})
        item = response.get('Item')

        if not item:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'Usuário não encontrado'})
            }

        return {
            'statusCode': 200,
            'body': json.dumps(item, default=decimal_default)
        }

    except Exception as e:
        print("❌ Erro ao buscar usuário:", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps({'message': 'Erro interno ao buscar usuário'})
        }

def get_spaces_by_hoster(event):
    """
    Lista espaços de um hoster.
    Aceita:
      - path:   GET /spaces/hoster/{userId}
      - query:  GET /spaces?hoster={userId}
    Usa GSI 'byHoster' (PK=hoster) se existir; caso contrário, faz Scan com filtro.
    """
    # 1) obter userId do path ou query
    path = (event.get('requestContext', {}).get('http', {}) or {}).get('path', '')
    query = event.get('queryStringParameters') or {}
    user_id = None

    # path style: /spaces/hoster/{userId}
    if "/spaces/hoster/" in path:
        user_id = path.split("/spaces/hoster/")[-1].strip("/")

    # query style: ?hoster=...
    if not user_id:
        user_id = (query.get('hoster') or "").strip()

    if not user_id:
        return {'statusCode': 400, 'body': json.dumps({'message': 'userId/hoster obrigatório'})}

    print("🔎 Buscando espaços do hoster:", user_id)

    # 2) tentar Query em GSI
    try:
        resp = table.query(
            IndexName='byHoster',              # GSI esperado (PK: hoster)
            KeyConditionExpression=Key('hoster').eq(user_id)
        )
        items = resp.get('Items', [])
        print(f"✅ Query em GSI byHoster retornou {len(items)} itens")
    except Exception as e:
        print("⚠️ GSI byHoster indisponível. Fazendo Scan com filtro. Motivo:", str(e))
        resp = table.scan(
            FilterExpression=Attr('hoster').eq(user_id)
        )
        items = resp.get('Items', [])
        print(f"📄 Scan com filtro (hoster={user_id}) retornou {len(items)} itens")

    # Conversões leves (mesma linha do get_available_coworking_spaces)
    norm = []
    for it in items:
        if 'amenities' in it and isinstance(it['amenities'], list):
            try:
                if all(isinstance(a, dict) and 'S' in a for a in it['amenities']):
                    it['amenities'] = [a['S'] for a in it['amenities']]
                elif not all(isinstance(a, str) for a in it['amenities']):
                    it['amenities'] = []
            except Exception:
                it['amenities'] = []

        try:
            it['precoHora'] = float(it.get('precoHora', 0) or 0)
            it['precoDia'] = float(it.get('precoDia', 0) or 0)
        except Exception:
            it['precoHora'] = 0
            it['precoDia'] = 0

        for campo in ['name', 'city', 'district']:
            if campo not in it:
                it[campo] = ""

        norm.append(it)

    return {'statusCode': 200, 'body': json.dumps(norm, default=decimal_default)}