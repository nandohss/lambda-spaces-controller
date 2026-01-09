import json
from coworking_spaces import (
    add_coworking_space,
    get_coworking_space,
    get_available_coworking_spaces,
    update_coworking_space,
    update_coworking_space_full,
    delete_coworking_space,
    get_user_by_id,
    get_spaces_by_hoster
)

def lambda_handler(event, context):
    print("🔎 EVENT RECEBIDO:", json.dumps(event))

    # Detectar formato do evento (REST API v1 vs HTTP API v2)
    try:
        if 'http' in event.get('requestContext', {}):
            # HTTP API (v2)
            http_method = event['requestContext']['http']['method']
            full_path = event['requestContext']['http']['path']
        else:
            # REST API (v1) - Padrão do SAM 'Type: Api'
            http_method = event['httpMethod']
            full_path = event['path']
    except KeyError:
        # Fallback ou erro
        print("❌ Formato de evento desconhecido")
        return {'statusCode': 400, 'body': "Invalid event format"}

    # Remove prefixo do estágio se presente (ex: /pro)
    stage = event.get('requestContext', {}).get('stage', '')
    if stage and full_path.startswith(f"/{stage}"):
        resource = full_path[len(f"/{stage}"):]
    else:
        resource = full_path

    route_key = f"{http_method} {resource}"
    
    print("📌 http_method:", http_method)
    print("📌 full_path:", full_path)
    print("📌 resource:", resource)
    print("📌 route_key:", route_key)

    # Coworking spaces
    # 1) rota nova por PATH: /spaces/hoster/{userId}
    if http_method == 'GET' and resource.startswith('/spaces/hoster/'):
        return get_spaces_by_hoster(event)

    # 2) rota existente /spaces com ramificações
    if route_key == 'GET /spaces':
        q = event.get('queryStringParameters') or {}
        if 'spaceId' in q:
            return get_coworking_space(event)
        # OPCIONAL: também aceitar ?hoster=... via query na mesma rota
        if 'hoster' in q:
            return get_spaces_by_hoster(event)
        return get_available_coworking_spaces(event)

    elif route_key == 'POST /spaces':
        return add_coworking_space(event)
    elif route_key == 'PUT /spaces':
        return update_coworking_space(event)
    elif route_key == 'DELETE /spaces':
        return delete_coworking_space(event)
    elif http_method == 'GET' and resource.startswith('/users/'):
        user_id = resource.replace('/users/', '')
        event['pathParameters'] = {'userId': user_id}
        return get_user_by_id(event)
    elif route_key == 'PUT /spaces/full':
      return update_coworking_space_full(event)

    # Rota não reconhecida
    return {
        'statusCode': 400,
        'body': json.dumps(f"Unsupported method or route: {route_key}")
    }
