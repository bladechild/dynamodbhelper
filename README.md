This package is for aws dynamodb user.
It helps developer to CRUD the dynamodb easily

Any bugs reported are welcomed

To import this package using "npm install dynamodbhelper" or "yarn add dynamodbhelper"

Before you use it, please add "const dbHelper = require('dynamodbhelper');"
<ul>
<li>
InsertItem: create or update one item in table
Examples:
dbHelper.InsertItem({id,name},'tablename',callback);
</li>
<li>
InsertItems: create or update multiple items
Examples:
dbHelper.InsertItems([{id,name1},{id,name2}],'tablename',callback);
</li>
<li>
DeleteItems: delete items (only pass an array of keys)
Examples:
dbHelper.DeleteItems([{id},{id}],'tablename',callback);
</li>
<li>
GetItemById: retrieve one item by the key
Examples:
dbHelper.GetItemById({id},'tablename').then(item=>{...}).catch(error=>...);
</li>
<li>
GetAllItems: retrieve all items which satisfy the conditions, conditions should be strictly follow the format: 
    {operator:"OR",conditions:["Feild1 > :para1","Feild2 < :para2","Feild3 != :para3"],parameters:{":para1":1,":para2":2,":para3":3}}
Examples:
dbHelper.GetAllItems('tablename',condition).then(items=>{...}).catch(error=>...);
</li>
<li>
UpdateItem: update existing item (add or update attribute)
Examples:
dbHelper.UpdateItem({id},{Feild1:value1,Feild2:value2},'tablename').then(item=>{...}).catch(error=>...);
</ul>