const fetch = require('node-fetch')
const MongoClient = require('mongodb').MongoClient
const getAccessToken = require('./get-access-token')
const { BITBUCKET_WORKSPACE } = process.env

const mongoString =
'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false'
const client = new MongoClient(mongoString)

const getPRCommits = async (workspace, repo_slug, pr_id, page = 1, agg = []) => {
    const url = `https://api.bitbucket.org/2.0/repositories/${workspace}/${repo_slug}/pullrequests/${pr_id}/commits?${page > 1 ? `page=${page}` : ''}`
    // console.log(url)
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
    // console.log(json)

    if (Array.isArray(json.values)) {
        const retval = [...json.values, ...agg]
        console.log(`Retrieved page ${page} containing ${json.values.length} items, next URL is ${json.next}`)
        console.log(`Collected ${retval.length} items so far`)

        if (json.next) return getPRCommits(workspace, repo_slug, pr_id, page + 1, retval)
        else return retval
    } else {
        console.log(`PR ID ${pr_id} for repo ${repo_slug} had no commits - ${url}`)
        return []
    }
}

const getPullRequestsFromMongo = async () => {
    try {
        await client.connect()

        console.log(`Getting prs from Mongo`)

        const database = client.db('bitbucket')
        const pulls = database.collection('pulls')
        
        const result = await (await pulls.find({})).toArray()

        console.log( `Retrieved ${result.length} PRs from Mongo`)
        
        return result
    } finally {
        await client.close()
    }
}

const savePRCommitsToMongo = async (slug, pr_id, commits) => {
    try {
        await client.connect()


        const database = client.db('bitbucket')
        const pulls = database.collection('pulls')
        
        console.log(`Saving ${commits.length} commits to PR ${pr_id} for repo ${slug} to Mongo`)

        const filter = { slug: slug, id: pr_id }
        const options = { upsert: true }

        const updateDoc = { $set: { commits  } }
        const result = await pulls.updateOne(filter, updateDoc, options)

        console.log( `${result.matchedCount} document(s) matched the filter, updated ${result.modifiedCount} document(s)`);
        
    } finally {
        await client.close()
    }
}

const getAndSavePRCommits = async workspace => {
    const pulls = await getPullRequestsFromMongo()
    
    for (const pull of pulls) {
        const pullRequestCommits = await getPRCommits(workspace, pull.slug, pull.id)
        
        await savePRCommitsToMongo(pull.slug, pull.id, pullRequestCommits)
        
        console.log(`Finished with ${pull.slug} #${pull.id}`)
    }


    console.log('Done, exiting')
    process.exit()
}

getAndSavePRCommits(BITBUCKET_WORKSPACE).then(res => res)
