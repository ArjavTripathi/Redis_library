from Client import Client
client = Client()

print(client.mset('k1', 'v1', 'k2', ['v2-0', 1, 'v2-2'], 'k3', 'v3'))

print(client.get('k2'))