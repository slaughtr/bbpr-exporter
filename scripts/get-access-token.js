const MongoClient = require('mongodb').MongoClient

const uri =
    'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false'

const client = new MongoClient(uri)

// Assumes you have your access_token in a config collection
// Token refresh outside the scope of this repo
const getAccessToken = async () => {
    try {
        await client.connect()
        const database = client.db('bitbucket')
        const configColl = database.collection('config')

        const config = await (await configColl.find({})).toArray()
        const accessToken = config[0].access_token

        return accessToken
    } finally {
        // Ensures that the client will close when you finish/error
        await client.close()
    }
}

module.exports = getAccessToken
