const { BloomFilter } = require('bloom-filters')
const { getNewId, getBloomFilterAccuracy } = require('./utils')
const chalk = require('chalk')
const { MongoClient } = require('mongodb')

const conenctToMongodb = async () => {
    const connectionUrl = 'mongodb://127.0.0.1:27017'
    const client = new MongoClient(connectionUrl)
    await client.connect()
    return client
}

const focusPrint = text => {
    return chalk.green.bold(text)
}

const testBloomFilterForUser = async (userId) => {
    const startTime = performance.now()

    const mongoClient = await conenctToMongodb()

    const sampleSize = 1000
    const filterSize = 12 * 10 ** 6
    const filter = new BloomFilter(filterSize, 8)
    const ids = []

    for (let i = 0; i < sampleSize; i++) {
        let newId = getNewId()
        ids.push(newId)
        filter.add(newId)
    }

    const bloomFilterData = filter.saveAsJSON()
    
    const db = mongoClient.db('users')
    const bf = db.collection('bloomFilters')
    await bf.insertOne({ filter: bloomFilterData, size: filterSize, userId })
    
    const bloomFilterAccuracy = getBloomFilterAccuracy(filter, ids)

    console.log('Bloom Filter accuracy for user', userId, 'is:', bloomFilterAccuracy)

    const endTime = performance.now()

    if (userId === 1) {
        console.log('Start time for first user: ', focusPrint(startTime))
        console.log('Total time for first user: ', focusPrint(endTime - startTime))
    }

    if (userId === 50) {
        console.log('Start time for last user: ', focusPrint(startTime))
        console.log('Total end time: ', focusPrint(endTime))
        process.exit()
    }
}

const runBloomFilterTest = async () => {
    const numberOfUsers = 50

    const userIds = []
    for (let i = 0; i < numberOfUsers; i++) {
        userIds.push(i + 1)
    }

    console.log(chalk.inverse('Number of users: '), focusPrint(numberOfUsers))

    userIds.forEach(userId => testBloomFilterForUser(userId))
}

runBloomFilterTest()