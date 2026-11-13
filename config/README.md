# Configuración de transporte

`transport_config.example.json` muestra la FORMA del archivo. Sus nombres de proveedor y sus valores (2/5/7 s y 5/15/20 s) son ilustrativos: salen de la propuesta
de configuración recibida, no del código ni de mediciones. Cada despliegue debe declarar los suyos en su propio archivo (p. ej. `transport_config.json`, fuera del
control de versiones si contiene datos de entorno) y cargarlo al arrancar con `TransportConfig.from_file(...)`.

- Las claves son los `provider_id` de UNAE (los del `keyring` y de `DispatchAuthorization`).
- Un proveedor que no figura en la configuración NO se despacha: `ConfigurationError` antes de tocar ningún Store.
- El despachador impone `total_timeout_s`; `connect_timeout_s` y `read_timeout_s` llegan al código del proveedor en `request["timeouts"]`.
- Para pruebas: `TransportConfig.from_dict({"providers": {...}})`, incluido `{"providers": {}}`.
