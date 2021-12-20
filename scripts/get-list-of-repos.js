const fetch = require('node-fetch')
const MongoClient = require('mongodb').MongoClient
const getAccessToken = require('./get-access-token')
const { BITBUCKET_WORKSPACE } = process.env

const mongoString =
    'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false'
const client = new MongoClient(mongoString)

const getListOfRepos = async (workspace, page, agg = []) => {
    const url = 'https://api.bitbucket.org/2.0/repositories/' + workspace + '?page=' + page
    const access_token = await getAccessToken()
    const fetchOptions = {
        method: 'GET',
        headers: {
            Accept: 'application/json',
            Authorization: 'Bearer ' + access_token,
        },
    }

    const response = await fetch(url, fetchOptions)
    const json = await response.json()

    const retval = [...json.values, ...agg]
    console.log(`Retrieved page ${page} containing ${json.values.length} items, next URL is ${json.next}`)
    console.log(`Collected ${retval} items so far`)

    if (json.next) return getListOfRepos(workspace, page + 1, retval)
    else return retval
}

const saveReposToMongo = async repos => {
    try {
        await client.connect()

        console.log(`Saving repos to Mongo`)

        const database = client.db('bitbucket')
        const repos = database.collection('repos')
        const result = await repos.insertMany(repos)

        console.log(`${result.insertedCount} repos inserted into Mongo`)
    } finally {
        await client.close()
    }
}

const getAndSaveRepos = async workspace => {
    console.log('Getting repos for workspace ', workspace)
    const repos = await getListOfRepos(workspace, 1)
    console.log(`Retrieved ${repos.length} repos`)

    await saveReposToMongo(repos)

    console.log('Done, exiting')
    process.exit()
}

getAndSaveRepos(BITBUCKET_WORKSPACE).then(res => res)