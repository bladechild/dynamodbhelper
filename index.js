'use strict';
var AWS = require('aws-sdk');

let count = 0;


function Query(tableName, items, start, condition, size = null, index, key, region = "us-east-1") {
    var defer = Promise.defer();
    //condition = {operator:and,conditions:["a>:para1","b<:para2","c!=:para3"],parameters:{":para1":0,":para2":0,":para3":0}}
    let paras = {
        TableName: tableName,
        Limit: 50
    };
    console.log(condition);
    if (condition) {
        let { conditions, operator, parameters, parameterNames } = condition;
        let filter = conditions.join(` ${operator} `);
        paras.FilterExpression = filter;
        paras.ExpressionAttributeValues = parameters;
        paras.ExpressionAttributeNames = parameterNames;
    }
    if (key) {
        paras.KeyConditions = {};
        Object.keys(key).forEach(keyCondition => {
            paras.KeyConditions[keyCondition] = {
                "ComparisonOperator": key[keyCondition][0],
                "AttributeValueList": key[keyCondition][1]
            }
        });
        console.log(paras.KeyConditions);
    }
    if (start)
        paras.ExclusiveStartKey = start;

    if (index) {
        paras.IndexName = index;
        paras.ScanIndexForward = false
    }
    // console.log(paras);
    const docClient = new AWS.DynamoDB.DocumentClient({ region });
    docClient.query(paras, (err, data) => {
        if (err) {
            console.log(`Fail fetching data from ${tableName}`, err);
            defer.reject(err);
        } else {
            //console.log(data);
            if (size) {
                if (items.length + data.Count < size) {
                    items.push(...data.Items);
                    if (data.LastEvaluatedKey) {
                        console.log("Next 100 ", items.length, size);
                        Query(tableName, items, data.LastEvaluatedKey, condition, size, index, key, region).then(defer.resolve).catch(defer.reject);
                    }
                    else {
                        console.log("done");
                        defer.resolve({
                            items,
                            lastEvaluatedKey: data.LastEvaluatedKey
                        });
                    }
                }
                else {
                    console.log("Last");
                    let rest = size - items.length;
                    items.push(...data.Items.slice(0, rest));
                    defer.resolve({
                        items,
                        lastEvaluatedKey: data.LastEvaluatedKey
                    });
                }
            }
            else {
                console.log("No Size");
                items.push(...data.Items);
                defer.resolve({
                    items,
                    lastEvaluatedKey: data.LastEvaluatedKey
                });
            }
        }
    });
    return defer.promise;
}

function GetItems(tableName, items, start, condition, size = null, index, region = "us-east-1") {
    var defer = Promise.defer();
    //condition = {operator:and,conditions:["a>:para1","b<:para2","c!=:para3"],parameters:{":para1":0,":para2":0,":para3":0}}
    let paras = {
        TableName: tableName,
        Limit: 50
    };
    if (condition) {
        let { conditions, operator, parameters, parameterNames } = condition;
        let filter = conditions.join(` ${operator} `);
        paras.FilterExpression = filter;
        paras.ExpressionAttributeValues = parameters;
        paras.ExpressionAttributeNames = parameterNames;
    }

    if (start)
        paras.ExclusiveStartKey = start;

    if (index) {
        paras.IndexName = index;
        paras.ScanIndexForward = false
    }
    // console.log(paras);
    const docClient = new AWS.DynamoDB.DocumentClient({ region });
    docClient.scan(paras, (err, data) => {
        if (err) {
            console.log(`Fail fetching data from ${tableName}`, err);
            defer.reject(err);
        } else {
            //console.log(data);
            if (size) {
                if (items.length + data.Count < size) {
                    items.push(...data.Items);
                    if (data.LastEvaluatedKey) {
                        console.log("Next 100 ", items.length, size);
                        GetItems(tableName, items, data.LastEvaluatedKey, condition, size, index, region).then(defer.resolve).catch(defer.reject);
                    }
                    else {
                        console.log("done");
                        defer.resolve({
                            items,
                            lastEvaluatedKey: data.LastEvaluatedKey
                        });
                    }
                }
                else {
                    console.log("Last");
                    let rest = size - items.length;
                    items.push(...data.Items.slice(0, rest));
                    defer.resolve({
                        items,
                        lastEvaluatedKey: data.LastEvaluatedKey
                    });
                }
            }
            else {
                console.log("No Size");
                items.push(...data.Items);
                defer.resolve({
                    items,
                    lastEvaluatedKey: data.LastEvaluatedKey
                });
            }
        }
    });
    return defer.promise;
}

function RecursiveGet(tableName, items, start, condition, callback, size = null, region = "us-east-1") {
    //condition = {operator:and,conditions:["a>:para1","b<:para2","c!=:para3"],parameters:{":para1":0,":para2":0,":para3":0}}
    let paras = {
        TableName: tableName,
        Limit: 50
    };
    if (condition) {
        let { conditions, operator, parameters, parameterNames } = condition;
        let filter = conditions.join(` ${operator} `);
        paras.FilterExpression = filter;
        paras.ExpressionAttributeValues = parameters;
        paras.ExpressionAttributeNames = parameterNames;
    }
    if (start)
        paras.ExclusiveStartKey = start;

    //console.log(paras);
    count += 1;
    // setTimeout(function () {
    const docClient = new AWS.DynamoDB.DocumentClient({ region });
    docClient.scan(paras, (err, data) => {
        count -= 1;
        if (err) {
            console.log(`Fail fetching data from ${tableName}`, err);
            callback("Error");
        } else {
            //console.log(data);
            if (size) {
                if (items.length + data.Count <= size) {
                    items.push(...data.Items);
                    if (data.LastEvaluatedKey)
                        RecursiveGet(tableName, items, data.LastEvaluatedKey, condition, callback, size, region);
                    else
                        callback("Done");
                }
                else {
                    let rest = size - items.length;
                    items.push(...data.Items.slice(0, rest));
                    callback("Done");
                }
            }
            else {
                items.push(...data.Items);
                if (data.LastEvaluatedKey)
                    RecursiveGet(tableName, items, data.LastEvaluatedKey, condition, callback, null, region);
                else
                    callback("Done");
            }
        }
    });
    // }, 250 * count);


}

function write(items, tableName, requestType, region = "us-east-1") {
    let paras = {};
    let tableparas = {};
    let itemsparas = items.map((Item) => {
        let itemrequest = {};
        if (requestType == "PutRequest")
            itemrequest[requestType] = { Item };
        else
            itemrequest[requestType] = { Key: Item };
        return itemrequest;
    });
    tableparas[tableName] = itemsparas;
    paras.RequestItems = tableparas;
    return new Promise((resolve, reject) => {
        const docClient = new AWS.DynamoDB.DocumentClient({ region });
        recursiveBatchWrite(docClient, paras, tableName, requestType, (error, msg) => {
            if (error)
                reject(error);
            else
                resolve(msg);
        });

    });

}
function recursiveBatchWrite(docClient, paras, tableName, requestType, callback) {
    docClient.batchWrite(paras, function (err, data) {
        let writeType = requestType == "PutRequest" ? "Insert" : "Delete";
        if (err) {
            callback(new Error(`Fail to ${writeType} records to table ${tableName}: Error: ${err}`));
        } else {
            if (data.UnprocessedItems[tableName]) {
                paras.RequestItems = data.UnprocessedItems;
                recursiveBatchWrite(docClient, paras, tableName, requestType, callback);
            }
            else
                callback(null, `${writeType} Records successfully in table ${tableName}`);
        }
    });
}

function recursiveWrite(items, tableName, requestType, callback, region = "us-east-1") {
    if (items.length <= 0)
        callback("Done");
    else {
        if (items.length <= 25) {
            write(items, tableName, requestType, region)
                .then(success => {
                    //console.log(success);
                    callback("Done");
                })
                .catch(error => {
                    //console.log(error);
                    callback(error);
                });

        }
        else {
            let subItems = items.slice(0, 25);
            write(subItems, tableName, requestType, region)
                .then(success => {
                    //console.log(success);
                    recursiveWrite(items.slice(25), tableName, requestType, callback, region);
                })
                .catch(error => {
                    //console.log(error);
                    callback(error);
                });
        }
    }
}

//create/update only one item
function InsertItem(item, tableName, callback, region = "us-east-1") {
    const params = {
        Item: item,
        TableName: tableName,
    };
    const docClient = new AWS.DynamoDB.DocumentClient({ region });
    docClient.put(params, function (err) {
        if (err) {
            ////console.log(`Fail inserting record to table ${tableName}: `, err);
            callback(err);
        } else {
            ////console.log(`Record inserted successfully to table ${tableName}`);
            callback(null, `Record inserted successfully to table ${tableName}`);
        }
    });
}

//create/update multiple items
function InsertItems(items, tableName, callback, region = "us-east-1") {
    recursiveWrite(items, tableName, "PutRequest", (msg) => callback(msg), region);
}

//delete items
function DeleteItems(items, tableName, callback, region = "us-east-1") {
    recursiveWrite(items, tableName, "DeleteRequest", (msg) => callback(msg), region);
}

//read item by key
function GetItemById(item, tableName, region = "us-east-1") {
    var params = {
        Key: item,
        TableName: tableName,
    };
    return new Promise((resolve, reject) => {
        const docClient = new AWS.DynamoDB.DocumentClient({ region });
        docClient.get(params, function (err, data) {
            if (err) {
                reject(`Fail fetching data from ${tableName},Error: ${err}`);
            } else {
                resolve(data.Item);
            }
        });
    });

}

//read all items, conditions is optional and must be well formated
function GetAllItems(tableName, condition = null, region = "us-east-1", size = null) {
    let items = [];
    return new Promise((resolve, reject) => {
        RecursiveGet(tableName, items, null, condition, (msg) => {
            if (msg == "Error")
                reject(`Fail fetching data from ${tableName}.`);
            else
                resolve(items);
        }, size, region);
    });

}

//update one item
function UpdateItem(key, attr, tableName, region = "us-east-1") {
    let expression = [];
    let expressionNames = {};
    let expressionValues = {};
    Object.keys(attr).forEach((key, index) => {
        expressionNames[`#para${index}`] = key;
        expressionValues[`:val${index}`] = attr[key];
        expression.push(`#para${index} = :val${index}`);
    });
    const params = {
        Key: key,
        TableName: tableName,
        ExpressionAttributeNames: expressionNames,
        ExpressionAttributeValues: expressionValues,
        UpdateExpression: `SET ${expression}`
    };
    return new Promise((resolve, reject) => {
        const docClient = new AWS.DynamoDB.DocumentClient({ region });
        docClient.update(params, function (err, data) {
            if (err) {
                reject(`Fail updating data from ${tableName},Error: ${err}`);
            } else {
                //console.log(data);
                resolve(data.Item);
            }
        });
    });
}

module.exports = {
    InsertItem,
    InsertItems,
    DeleteItems,
    GetItemById,
    GetAllItems,
    UpdateItem,
    GetItems,
    Query
};