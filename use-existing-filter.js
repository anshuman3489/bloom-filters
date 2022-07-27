const { BloomFilter } = require('bloom-filters')
const { getBloomFilterAccuracy } = require('./utils')
const chalk = require('chalk')
const redis = require('redis')
const fs = require('fs')
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

const runTestWithRedis = async (sampleSize, filterSize) => {
    console.log(chalk.green.inverse('Test with Redis'))
    const startTime = performance.now()
    const redisClient = await connectToRedis()
    const redisConnectedAt = performance.now()
    console.log('Time taken to connect to redis:', focusPrint(redisConnectedAt - startTime))

    const idData = await redisClient.get(`ids-${filterSize}`)
    const idsReceivedAt = performance.now()
    const bloomFilterData = await redisClient.get(`filter-${filterSize}`)
    const bloomFilterReceivedAt = performance.now()
    console.log('Time taken to get bloom filter from redis:', focusPrint(bloomFilterReceivedAt - idsReceivedAt), 'ms')

    const ids = JSON.parse(idData)
    const idsParsedAt = performance.now()
    const filter = BloomFilter.fromJSON(JSON.parse(bloomFilterData))
    const bfParsedAt = performance.now()
    console.log('Time taken to parse bloom filter data:', focusPrint(bfParsedAt - idsParsedAt), 'ms')

    const bloomFilterAccuracy = getBloomFilterAccuracy(filter, ids)
    const accuracyCheckedAt = performance.now()
    console.log(`Time taken to check accuracy of ${sampleSize / 100} ids:`, focusPrint(accuracyCheckedAt - bfParsedAt), 'ms')
    console.log('Bloom Filter Accuracy:', bloomFilterAccuracy)
}

const runTestWithMongodb = async (sampleSize, filterSize) => {
    console.log(chalk.green.inverse('Test with Mongodb'))
    const startTime = performance.now()
    const mongoClient = await conenctToMongodb()
    const mongodbConnectedAt = performance.now()
    console.log('Time taken to connect to mongodb:', focusPrint(mongodbConnectedAt - startTime))

    const ids = JSON.parse(fs.readFileSync(`ids-${filterSize}.json`))
    const idsReceivedAt = performance.now()
    console.log('Time taken to get id list from file system and parse it:', focusPrint(idsReceivedAt - mongodbConnectedAt), 'ms')

    const db = mongoClient.db('users')
    const bf = db.collection('bloomFilters')
    const bloomFilterData = await bf.findOne({ size: filterSize })
    const bloomFilterReceivedAt = performance.now()
    console.log('Time taken to get bloom filter from mongodb:', focusPrint(bloomFilterReceivedAt - idsReceivedAt), 'ms')

    const filter = BloomFilter.fromJSON(bloomFilterData.filter)
    const bfParsedAt = performance.now()
    console.log('Time taken to parse bloom filter data:', focusPrint(bfParsedAt - bloomFilterReceivedAt), 'ms')

    const bloomFilterAccuracy = getBloomFilterAccuracy(filter, ids)
    const accuracyCheckedAt = performance.now()
    console.log(`Time taken to check accuracy of ${sampleSize / 100} ids:`, focusPrint(accuracyCheckedAt - bfParsedAt), 'ms')
    console.log('Bloom Filter Accuracy:', bloomFilterAccuracy)
}

const runBloomFilterTest = async () => {
    const sampleSize = 10 ** 6
    const filterSize = 12 * sampleSize
    
    await runTestWithRedis(sampleSize, filterSize)
    await runTestWithMongodb(sampleSize, filterSize)
    
    process.exit()
}

runBloomFilterTest()