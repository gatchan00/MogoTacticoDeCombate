from pymongo import MongoClient, UpdateOne

from mongo_cfg import mongoConfig, customersPerBulk, inputFile, lastCheckpoint, projectionFields



def createMongoConnection(mongoConfig):
    if mongoConfig['fullUrl']:
        print("Full URL connection")
        client = MongoClient(mongoConfig['fullUrl'])
        db = client[mongoConfig['db']]
    else:
        client = MongoClient(mongoConfig['host'], mongoConfig['port'])
        db = client[mongoConfig['db']]
    clientCol = db[mongoConfig['collectionClientes']]
    polizasCol = db[mongoConfig['collectionPolizas']]
    statusCol = db[mongoConfig['statusCol']]

    return (clientCol, polizasCol, statusCol)


def mainLoop(clientCol, polizasCol, statusCol, inputFile,lastCheckpoint,customersPerBulk, projectionFields):
    with open(inputFile, 'r') as f:
        currentCustomer=None
        totalCustomersProcessed = 0

        #Skip all customers before the lastCheckpoint. It must exist in the fil!!!!
        if lastCheckpoint:
            while lastCheckpoint:
                skipCustomer = int(f.readline())
                if skipCustomer == lastCheckpoint:
                    lastCheckpoint = None #All elements have been skipped now

        #bucle general, por cada cliente...
        #creo bulk vacío
        mongoOperations = []
        for currLine in f:
            currentCustomer = int(currLine)

            #Coger sus pólizas
            polizasCustomer = polizasCol.find({'COD_CLIENTE_RIC':currentCustomer}, projectionFields)
            listadoPolizas = [doc for doc in polizasCustomer]

            #Crear una operación update y Añadirla al bulk
            newMongoOp = UpdateOne({'idCliente': currentCustomer}, {'$addToSet': {'polizas': {'$each': listadoPolizas}}}, upsert=True)
            mongoOperations.append(newMongoOp)
            totalCustomersProcessed += 1

            #si llego al total de operacions, lanzo el bulk y lo reinicio
            if totalCustomersProcessed % customersPerBulk == 0:
                print("Begin to write batch for: "+str(totalCustomersProcessed))
                clientCol.bulk_write(mongoOperations)
                print("Finish writting batch for: " + str(totalCustomersProcessed))
                statusCol.update_one({'processId': 'MongoTacticoDeCombate'}, {'$set': {'latestCustomer': currentCustomer}}, upsert=True)
        #Al acabar, lanzo el bulk con las operaciones pendientes
        print("Begin to write final batch: "+str(totalCustomersProcessed))
        clientCol.bulk_write(mongoOperations)
        print("Finish writting final batch: " + str(totalCustomersProcessed))
        statusCol.update_one({'processId': 'MongoTacticoDeCombate'}, {'$set': {'latestCustomer': currentCustomer}}, upsert=True)

if __name__ == '__main__':
    (clientCol, polizasCol, statusCol) = createMongoConnection(mongoConfig)
    mainLoop(clientCol, polizasCol, statusCol, inputFile, lastCheckpoint, customersPerBulk, projectionFields)