const redis = require('redis')
const { getNewId } = require('./utils')

const getRandomInt = (min, max) => {
    const range = max + 1 - min
    return Math.floor(Math.random() * range) + min
}

const connectToRedis = async () => {
    const client = redis.createClient({
        url: 'redis://default:rUb73dskYUF9vlAZqF2sLlYeGaRpy9sJ@redis-17707.c212.ap-south-1-1.ec2.cloud.redislabs.com:17707'
    })
    await client.connect()
    return client
}

const testRedisBloomForUser = async userId => {
    const startTime = performance.now()

    const client = await connectToRedis()

    const sampleSize = 10 ** 5
    const errorRate = 0.01
    const bfName = `bf-${userId}`

    await client.del(bfName)

    try {
        await client.bf.reserve(bfName, errorRate, sampleSize)
        console.log('Reserved Bloom Filter.')
    } catch (e) {
        if (e.message.endsWith('item exists')) {
            console.log('Bloom Filter already reserved.');
        } else {
            console.log('Error, maybe RedisBloom is not installed?:');
            console.log(e);
        }
    }

    const ids = []
    for (let i = 0; i < sampleSize; i++) {
        let newId = getNewId()
        ids.push(newId)
        // await client.bf.add(bfName, newId)
    }

    await client.bf.mAdd(bfName, ids)

    const idsToCheck = []
    const answers = []

    for (let i = 0; i < sampleSize / 10; i++) {
        const checkExistingId = getRandomInt(0, 1)
        if (checkExistingId) {
            const index = getRandomInt(0, sampleSize - 1)
            idsToCheck.push(ids[index])
        } else {
            idsToCheck.push(getNewId())
        }
        answers.push(checkExistingId)
    }

    const outputs = await client.bf.mExists(bfName, idsToCheck)

    let correctAnswer = 0

    for (let i = 0; i < outputs.length; i++) {
        if (outputs[i] == answers[i]) correctAnswer++
    }

    const accuracy = correctAnswer / (outputs.length) * 100

    const endTime = performance.now()

    console.log('Start time: ', startTime)
    console.log('End time: ', endTime)
    console.log('Total time: ', endTime - startTime)
    console.log('Accuracy: ', accuracy)

    // const info = await client.bf.info(bfName);
    // console.log(info);

    await client.quit();
}

const runBloomFilterTest = async () => {
    const numberOfUsers = 10

    const userIds = []
    for (let i = 0; i < numberOfUsers; i++) {
        userIds.push(i + 1)
    }

    userIds.forEach(userId => testRedisBloomForUser(userId))
}

runBloomFilterTest()