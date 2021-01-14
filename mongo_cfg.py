#CONFIG
inputFile = 'F:\\everis\\mongoaxa\\Mapfre-Sandbox\\Id_clientesOrd.txt'
lastCheckpoint = None
customersPerBulk = 20000
mongoConfig = {'fullUrl': None, #"mongodb+srv://mvp_ficha_compass:<password>@cluster-ric-mvp-tu2rr.azure.mongodb.net/<dbname>?retryWrites=true&w=majority"
               'host': 'localhost',
               'port': 27017,
               'db': 'RIC',
               'collectionClientes': 'CLIENTES',
               'collectionPolizas': 'RIC_POL_INTER',
               'statusCol': 'MongoTacticoDeCombateLog'}

projectionFields = {"_id":0, "codApli":1, "codCia":1, "codPoliza":1, "idnCliOri":1, "tipFigura":1, "CLEAN_TS":1, "COD_CIS":1, "COD_CLIENTE_RIC":1, "COD_ENTIDAD":1, "COD_USR":1, "CURRENT_TS":1, "FEC_ACTU":1, "FEC_VIGENCIA_DESDE":1, "FEC_VIGENCIA_HASTA":1, "IND_VISIBILIDAD":1, "KAFKA_IN_TS":1, "OP_TS":1, "insertedTS":1, "modifiedTS":1}