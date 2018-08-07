/*
 * Parameters
 * type: データタイプ。テーブル名が入る
 * data: テーブルにinsertするデータ。json形式で送る。
 */

exports.sendEvent2BQ = (req, res) => {
    res.header('Access-Control-Allow-Origin', '*')
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
    res.header('Access-Control-Allow-Methods', 'POST,PUT,PATH,OPTIONS')

    auth(req, res)
    let eventType = req.body.type || ""
    if(eventType.length == 0) {
        res.status = 400
        res.end("データタイプが設定されていません。")
    }

    const bq = require('gcloud')({ projectId: process.env.BQ_PROJECT_ID }).bigquery()

    let ds = null
    const datasetNames = ['users', 'user_payments']
    datasetNames.map(name => {
        if(eventType == name) {
            ds = bq.dataset(name)
        }
    })

    if(!ds) {
        res.status = 400
        res.end(`データタイプ'${eventType}'は不正です。`)
    }

    const targetTable = ds.table(TableUtil.getCurrentMonthTableName(eventType))

    targetTable.exists((err, exists) => {
        const insertData = JSON.parse(req.body.data)
        const cb = () => {
            TableOperator.insert2bq(targetTable, insertData, res)
        }
        if(!exists) {
            TableOperator.createTable(ds, res, eventType, cb)
        } else {
            cb()
        }
    })
}

const auth = (req, res) => {
    const authorization = req.get('Authorization')
    //「Authorization: Bearer 文字列」みたいな感じのヘッダーが設定されているのが正しいので
    // デコードしてそのままトークンと比較するのは本当は正しくないので注意。
    if(decodeBase64(authorization) != process.env.AUTH_TOKEN) {
        res.status = 401
        res.end('invalid auth token.')
    }
}

const decodeBase64 = (s) => {
    return new Buffer(s || "", 'base64')
}

class TableOperator {
    static schemes() {
        const utilParams = [
            {
                "type": "string",
                "mode": "required",
                "name": "id",
            },
            {
                "type": "string",
                "name": "os",
            },
            {
                "type": "string",
                "name": "city",
            },
            {
                "type": "string",
                "name": "referrer",
            },
            {
                "type": "string",
                "name": "initial_referrer",
            },
        ]
        return {
            users: {
                schema: {
                    "fields": [
                        {
                            "type": "timestamp",
                            "name": "deleted_at",
                        },
                        {
                            "type": "timestamp",
                            "mode": "required",
                            "name": "email",
                        },
                    ].concat(utilParams),
                },
            },
            user_payments: {
                schema: {
                    "fields": [
                        {
                            "type": "id",
                            "mode": "required",
                            "name": "id",
                        },
                        {
                            "type": "timestamp",
                            "mode": "required",
                            "name": "created_at",
                        },
                        {
                            "type": "timestamp",
                            "mode": "required",
                            "name": "deleted_at",
                        },
                        {
                            "type": "timestamp",
                            "mode": "required",
                            "name": "trial_start",
                        },
                        {
                            "type": "timestamp",
                            "mode": "required",
                            "name": "trial_end",
                        },
                        {
                            "type": "string",
                            "mode": "required",
                            "name": "card_type",
                        },
                        {
                            "type": "string",
                            "mode": "required",
                            "name": "card_last_four",
                        },
                        {
                            "type": "string",
                            "mode": "required",
                            "name": "stripe_customer_id",
                        },
                    ],
                },
            },
        }
    }

    static createTable(ds, res, eventType, cb) {
        const tableScheme = this.getScheme(eventType)
        const tableName = TableUtil.getCurrentMonthTableName(eventType)
        if(!tableScheme) {
            res.status = 500
            res.end(`Table scheme was not found by name: ${eventType}`)
            return
        }
        ds.createTable(tableName, tableScheme, (err, table, apiResponse) => {
            if ( err ) {
                console.log('err: ', err)
                console.log('apiResponse: ', apiResponse)
                res.status = 500
                res.end("TABLE CREATION FAILED:" + JSON.stringify(err))
                return
            } else {
                console.log("table created")
                cb()
            }
        })
    }

    static getScheme(name) {
        const scm = this.schemes()
        if(scm[name]) {
            return scm[name]
        }
        return null
    }

    static insert2bq(table, insertData, res) {
        const row = {
            insertId: (new Date()).getTime(),
            json: insertData
        }
        const options = {
            raw: true,
            skipInvalidRows: true,
        }
        table.insert(row, options, (err, insertErrors, apiResponse) => {
            if (err) {
                console.log('err: ', err)
                console.log('insertErr: ', insertErrors)
                console.log('apiResponse: ', JSON.stringify(apiResponse))
                res.status = 500
                res.end("FAILED:" + JSON.stringify(err) + JSON.stringify(insertErrors))
            } else {
                res.status = 200
                res.end(JSON.stringify(apiResponse))
            }
        })
    }

    static isValidScheme(name, data) {
        const scm = this.schemes(name)
        // ...WIP...
    }
}

class TableUtil {
    static getPostfix() {
        const d = new Date()
        let m = d.getMonth() + 1
        m = (m < 10) ? "0" + m : m
        return `${d.getFullYear()}${m}`
    }

    static getCurrentMonthTableName(name) {
        return `${name}_${this.getPostfix()}`
    }
}

