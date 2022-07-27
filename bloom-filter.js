const fs = require('fs')
const { BloomFilter } = require('bloom-filters')
const { getNewId, getBloomFilterAccuracy } = require('./utils')
const redis = require('redis')
const chalk = require('chalk')
const { MongoClient } = require('mongodb')

const connectToRedis = async () => {
    const client = redis.createClient()
    await client.connect()
    return client
}

const conenctToMongodb = async () => {
    const connectionUrl = 'mongodb://127.0.0.1:27017'
    const client = new MongoClient(connectionUrl)
    await client.connect()
    return client
}

const focusPrint = text => {
    return chalk.green.bold(text)
}

const runBloomFilterTest = async () => {
    const startTime = performance.now()

    const redisClient = await connectToRedis()
    const mongoClient = await conenctToMongodb()
    const dbsConnectedAt = performance.now()
    console.log('Time taken to connect to db clients: ', focusPrint(dbsConnectedAt - startTime), 'ms')

    const sampleSize = 10 ** 6
    const filterSize = 10 * sampleSize
    const filter = new BloomFilter(filterSize, 8)
    const ids = []
    const filterCreatedAt = performance.now()
    console.log(`Time taken to create bloom filter of size ${filterSize}:`, focusPrint(filterCreatedAt - dbsConnectedAt), 'ms')

    for (let i = 0; i < sampleSize; i++) {
        let newId = getNewId()
        ids.push(newId)
        filter.add(newId)
    }
    const idsInsertedAt = performance.now()
    console.log(`Time taken to insert ${sampleSize} ids:`, focusPrint(idsInsertedAt - filterCreatedAt), 'ms')

    const bloomFilterData = filter.saveAsJSON()
    const bfJsonCreatedAt = performance.now()
    console.log('Time taken to convert bf data to json:', focusPrint(bfJsonCreatedAt - idsInsertedAt), 'ms')
    
    const bloomFilterString = JSON.stringify(bloomFilterData)
    const dataStringifiedAt = performance.now()
    console.log('Time taken to stringify bf:', focusPrint(dataStringifiedAt - bfJsonCreatedAt), 'ms')
    
    fs.writeFileSync(`filter-${filterSize}.json`, bloomFilterString)
    const dataSavedLocallyAt = performance.now()
    console.log('Time taken to save bf in file system:', focusPrint(dataSavedLocallyAt - dataStringifiedAt), 'ms')

    await redisClient.set(`filter-${filterSize}`, bloomFilterString)
    const dataSavedInRedisAt = performance.now()
    console.log('Time taken to save bf in redis:', focusPrint(dataSavedInRedisAt - dataSavedLocallyAt), 'ms')

    const db = mongoClient.db('users')
    const bf = db.collection('bloomFilters')
    await bf.insertOne({ filter: bloomFilterData, size: filterSize })
    const dataSavedInMongodbAt = performance.now()
    console.log('Time taken to save bf in mongodb:', focusPrint(dataSavedInMongodbAt - dataSavedInRedisAt), 'ms')

    const idsString = JSON.stringify(ids)
    fs.writeFileSync(`ids-${filterSize}.json`, idsString)
    await redisClient.set(`ids-${filterSize}`, idsString)
    const idsSavedAt = performance.now()
    console.log('Time taken to save list of ids in file system:', focusPrint(idsSavedAt - dataSavedInMongodbAt), 'ms')

    const bloomFilterAccuracy = getBloomFilterAccuracy(filter, ids)
    const accuracyCheckedAt = performance.now()
    console.log(`Time taken to check accuracy for ${sampleSize / 100} ids:`, focusPrint(accuracyCheckedAt - idsSavedAt), 'ms')
    console.log('Bloom Filter Accuracy:', bloomFilterAccuracy)

    process.exit()
}

runBloomFilterTest()