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

    space_id = body['spaceId']
    name = body['name']
    city = body['city']
    country = body['country']
    district = body['district']
    capacity = body['capacity']
    amenities = body['amenities']
    availability = body['availability']
    hoster = body['hoster']

    categoria = body.get('categoria')
    subcategoria = body.get('subcategoria')
    descricao = body.get('descricao')
    regras = body.get('regras')
    dias_semana = body.get('diasSemana', [])
    hora_inicio = body.get('horaInicio')
    hora_fim = body.get('horaFim')
    preco_hora = float(body.get('precoHora', 0))
    preco_dia = float(body.get('precoDia', 0))

    item = {
        'spaceId': space_id,
        'name': name,
        'city': city,
        'country': country,
        'district': district,
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
        'precoDia': Decimal(str(preco_dia))
    }

    imagem_url = body.get('imagemUrl')
    if imagem_url:
        item['imagemUrl'] = imagem_url

    print("✅ Salvando item no DynamoDB:", json.dumps(item, default=decimal_default))
    table.put_item(Item=item)

    try:
        users_table.update_item(
            Key={'userId': hoster},  # Supondo que hoster seja o userId
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

    if 'name' in body:
        update_expression += " name = :n,"
        expression_attribute_values[':n'] = body['name']
    if 'city' in body:
        update_expression += " city = :c,"
        expression_attribute_values[':c'] = body['city']
    if 'country' in body:
        update_expression += " country = :co,"
        expression_attribute_values[':co'] = body['country']
    if 'district' in body:
        update_expression += " district = :d,"
        expression_attribute_values[':d'] = body['district']
    if 'capacity' in body:
        update_expression += " capacity = :ca,"
        expression_attribute_values[':ca'] = body['capacity']
    if 'amenities' in body:
        update_expression += " amenities = :a,"
        expression_attribute_values[':a'] = body['amenities']
    if 'availability' in body:
        update_expression += " availability = :av,"
        expression_attribute_values[':av'] = body['availability']
    if 'hoster' in body:
        update_expression += " hoster = :h,"
        expression_attribute_values[':h'] = body['hoster']
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
        # Teste rápido do GSI: se não existir, Dynamo levanta ValidationException
        resp = table.query(
            IndexName='byHoster',              # GSI esperado (PK: hoster)
            KeyConditionExpression=Key('hoster').eq(user_id)
        )
        items = resp.get('Items', [])
        print(f"✅ Query em GSI byHoster retornou {len(items)} itens")
    except Exception as e:
        # Fallback: Scan com filtro
        print("⚠️ GSI byHoster indisponível. Fazendo Scan com filtro. Motivo:", str(e))
        resp = table.scan(
            FilterExpression=Attr('hoster').eq(user_id)
        )
        items = resp.get('Items', [])
        print(f"📄 Scan com filtro (hoster={user_id}) retornou {len(items)} itens")

    # Conversões leves (mesma linha do get_available_coworking_spaces)
    norm = []
    for it in items:
        # amenities pode vir como lista de mapas/strings
        if 'amenities' in it and isinstance(it['amenities'], list):
            try:
                if all(isinstance(a, dict) and 'S' in a for a in it['amenities']):
                    it['amenities'] = [a['S'] for a in it['amenities']]
                elif not all(isinstance(a, str) for a in it['amenities']):
                    it['amenities'] = []
            except Exception:
                it['amenities'] = []

        # preços para float (o app trata como Double)
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