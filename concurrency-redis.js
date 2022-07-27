const { BloomFilter } = require('bloom-filters')
const { getNewId, getBloomFilterAccuracy } = require('./utils')
const redis = require('redis')
const chalk = require('chalk')

const connectToRedis = async () => {
    const client = redis.createClient()
    await client.connect()
    return client
}

const focusPrint = text => {
    return chalk.green.bold(text)
}

const testBloomFilterForUser = async (userId) => {
    const startTime = performance.now()

    const redisClient = await connectToRedis()

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

    const bloomFilterString = JSON.stringify(bloomFilterData)
    
    await redisClient.set(`user-${userId}-filter-${filterSize}`, bloomFilterString)

    const idsString = JSON.stringify(ids)
    await redisClient.set(`user-${userId}-ids-${sampleSize}`, idsString)

    const bloomFilterAccuracy = getBloomFilterAccuracy(filter, ids)

    console.log('Bloom Filter accuracy for user', userId, 'is:', bloomFilterAccuracy)

    const endTime = performance.now()

    if (userId === 1) {
        console.log('Start time for first user: ', focusPrint(startTime))
        console.log('Total time for first user: ', focusPrint(endTime - startTime))
    }

    if (userId === 100) {
        console.log('Start time for last user: ', focusPrint(startTime))
        console.log('Total time for all users: ', focusPrint(endTime))
        process.exit()
    }
}

const runBloomFilterTest = async () => {
    const numberOfUsers = 100

    const userIds = []
    for (let i = 0; i < numberOfUsers; i++) {
        userIds.push(i + 1)
    }

    userIds.forEach(userId => testBloomFilterForUser(userId))
}

runBloomFilterTest()