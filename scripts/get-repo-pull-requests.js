const fetch = require('node-fetch')
const MongoClient = require('mongodb').MongoClient
const getAccessToken = require('./get-access-token')
const { BITBUCKET_WORKSPACE } = process.env

const mongoString =
'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false'
const client = new MongoClient(mongoString)

const getRepoPullRequests = async (workspace, repo_slug, page = 1, agg = []) => {
    const url = `https://api.bitbucket.org/2.0/repositories/${workspace}/${repo_slug}/pullrequests?state=MERGED&state=SUPERSEDED&state=OPEN&state=DECLINED&page=${page}`
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
    console.log(`Collected ${retval.length} items so far`)

    if (json.next) return getRepoPullRequests(workspace, repo_slug, page + 1, retval)
    else return retval
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

const saveRepoPullRequestsToMongo = async (slug, pullRequests) => {
    try {
        await client.connect()


        const database = client.db('bitbucket')
        const pulls = database.collection('pulls')
        
        for (const pull of pullRequests) {
            console.log(`Saving PR ${pull.title} for repo ${slug} to Mongo`)

            const doc = { ...pull, slug }

            const result = await pulls.insertOne(doc)
            console.log(`A document was inserted with the _id: ${result.insertedId}`);
        }
        
    } finally {
        await client.close()
    }
}

const getAndSaveRepoPullRequests = async workspace => {
    const repos = await getReposFromMongo()
    
    for (const repo of repos) {
        const repoPullRequests = await getRepoPullRequests(workspace, repo.slug)
        
        await saveRepoPullRequestsToMongo(repo.slug, repoPullRequests)
        
        console.log(`Finished with ${repo.slug}`)
    }


    console.log('Done, exiting')
    process.exit()
}

getAndSaveRepoPullRequests(BITBUCKET_WORKSPACE).then(res => res)