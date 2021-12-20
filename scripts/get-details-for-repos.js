const fetch = require('node-fetch')
const MongoClient = require('mongodb').MongoClient
const getAccessToken = require('./get-access-token')
const { BITBUCKET_WORKSPACE } = process.env

const mongoString =
'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false'
const client = new MongoClient(mongoString)

const getRepoDetails = async (workspace, repo_slug) => {
    const url = `https://api.bitbucket.org/2.0/repositories/${workspace}/${repo_slug}`
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


    console.log(`Retrieved repo ${repo_slug}`)
    return json
}

const getReposFromMongo = async () => {
    try {
        await client.connect()

        console.log(`Getting repos from Mongo`)

        const database = client.db('bitbucket')
        const repos = database.collection('repos')
        
        const result = await (await repos.find({})).toArray()

        console.log( `Retrieved ${result.length} repos from Mongo`)
        
        return result
    } finally {
        await client.close()
    }
}

const saveRepoDetailsToMongo = async repo => {
    try {
        await client.connect()

        console.log(`Saving repo ${repo.slug} to Mongo`)

        const database = client.db('bitbucket')
        const repos = database.collection('repos')
        
        const filter = { slug: repo.slug }
        const options = { upsert: true }

        const updateDoc = { '$set': { ...repo } }
        const result = await repos.updateOne(filter, updateDoc, options)

        console.log( `${result.matchedCount} document(s) matched the filter, updated ${result.modifiedCount} document(s)`);
    } finally {
        await client.close()
    }
}

const getAndSaveRepoDetails = async workspace => {
    const repos = await getReposFromMongo()
    
    for (const repo of repos) {
        const repoDetails = await getRepoDetails(workspace, repo.slug)
        
        const updated = {...repo, ...repoDetails}

        await saveRepoDetailsToMongo(updated)
        
        console.log(`Finished with ${repo.slug}`)
    }


    console.log('Done, exiting')
    process.exit()
}

getAndSaveRepoDetails(BITBUCKET_WORKSPACE).then(res => res)