const { ObjectId } = require('mongodb')
// const { v4: uuid } = require('uuid')

const getRandomInt = (min, max) => {
    const range = max + 1 - min
    return Math.floor(Math.random() * range) + min
}

const getNewId = () => {
    let newId = new ObjectId();
    return newId.toString();
}

const getBloomFilterAccuracy = (filter, existingIds) => {
    let correctAnswer = 0
    const sampleSize = existingIds.length
    for (let i = 0; i < sampleSize / 100; i++) {
        const checkExistingId = getRandomInt(0, 1)
        let randomIdToCheck = ''
        if (checkExistingId) {
            const index = getRandomInt(0, sampleSize - 1)
            randomIdToCheck = existingIds[index]
        } else {
            randomIdToCheck = getNewId()
        }

        if (filter.has(randomIdToCheck) == checkExistingId) correctAnswer += 1
    }

    const accuracy = correctAnswer / (sampleSize / 100) * 100
    return accuracy
}

module.exports = { getNewId, getBloomFilterAccuracy }