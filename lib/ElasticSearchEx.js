var Util = require('util');

//var ClassUtil = require('../lib/class-util');
function bindFunctions(obj, bindObjAsFirstArgumentAlso)
{
    var m;
    for (var i in obj)
    {
        if ((typeof obj[i] == 'function') && (m = i.match(/^__(.*)$/)))
        {
            obj[m[1]] = (bindObjAsFirstArgumentAlso === true) ? obj[i].bind(obj, obj) : obj[i].bind(obj);
        }
    }
}



function ElasticSearchEx(es, options)
{
    var me = this;
    me.client = es;
    me.options = options || {};
    /*ClassUtil.*/bindFunctions(me, true);
}

ElasticSearchEx.prototype.__clone = function (me, obj)
{
    return JSON.parse(JSON.stringify(obj));
}

ElasticSearchEx.prototype.__makeOptions = function (me, moreOptions)
{
    var o = me.clone(me.options);
    for (var i in moreOptions) o[i] = moreOptions[i];
    return o;
}

ElasticSearchEx.prototype.__select = function (me, options)
{
    return new me.constructor(me.client, me.makeOptions(options));
}

ElasticSearchEx.prototype.__optionifyId = function (me, id)
{
    return (typeof id != 'object') ? me.makeOptions({ _id: id }) : id;
}

ElasticSearchEx.prototype.__get = function (me, id, callback)
{
    id = me.optionifyId(id);
    me.client.get(id, callback);
}

ElasticSearchEx.prototype.__search = function (me, options, query, callback)
{
    if (callback == null && typeof query == 'function')
    {
        callback = query;
        query = options;
        options = me.options;
    }
    me.client.search(options, query, callback);
}

ElasticSearchEx.prototype.__update = function (me, id, conds, doc, callback)
{
    id = me.optionifyId(id);

    // just in case conditions weren't set:
    if (callback == null || typeof doc == 'function')
    {
        callback = doc;
        doc = conds;
        conds = null;
    }

    if (conds == null)
    {
        me.client.update(id, { doc: doc }, callback);
    }
    else
    {
        // create MVEL script
        var j = 0;
        var params = {};
        var test = "";
        var glue = "";

        // logically AND all conditions together (room from improvement)
        for (var i in conds)
        {
            j++;
            test += glue;
            test += "ctx._source." + i + " == c" + j;
            params["c" + j] = conds[i];
            glue = " && ";
        }

        // values
        var setters = "";
        j = 0;
        for (var i in doc)
        {
            j++;
            setters += "  ctx._source." + i + " = v" + j + ";\n";
            params["v" + j] = doc[i]; // dotTraverseObject(doc, i);
        }
        var script = "if (" + test + ")\n{\n" + setters + "}";

        // off we go!
        me.client.update(id, { script: script, params: params }, callback);
    }
}

ElasticSearchEx.prototype.__updateAndGet = function (me, id, conds, doc, callback)
{
    id = me.optionifyId(id);

    me.update(id, conds, doc, function (err, res)
    {
        if (err) callback(err);
        else me.get(id, callback);
    });
}

ElasticSearchEx.prototype.__delete = function (me, id, callback)
{
    id = me.optionifyId(id);
    me.client.delete(id, callback);
}

ElasticSearchEx.prototype.__buildQuery = function(me, conds)
{
    var obj = // default is AND operation
        {
            'bool':
                {
                    'must': []
                }
        };

    for (var key in conds)
    {
        var value = conds[key];

        if (key == '$or')
        {
            if (!(value instanceof Array)) throw new Error("$or must be matched against an array");
            var bool =
                {
                    'should': [],
                    'minimum_number_should_match': 1
                }
            for (var i = 0; value.length > i; i++)
            {
                bool.should.push(me.buildQuery(value[i]));
            }
            obj.bool.must.push({ 'bool': bool });
        }
        else if (key == '$and')
        {
            for (var i = 0; value.length > i; i++)
            {
                obj.bool.must.push(me.buildQuery(value[i]));
            }
        }
        else if (key == '$not')
        {
            throw new Error("$not not yet implemented");
        }
        else if (key == '$nor')
        {
            throw new Error("$nor not yet implemented");
        }
        else
        {
            var value = conds[key];
            if (typeof value == 'string' || typeof value == 'number' || value === null)
            {
                var o = {};
                o[key] = value;
                obj.bool.must.push({ term: o });
            }
            else if (typeof value == 'object')
            {
                // $where -> not possible, requires scripting
                // $mod = [ divisor, remainder ] = if field % divisor = remainder
                // $nin
                // $or 
                // $gte, $lte, $lt, $gt could be matched to range queries: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
                // $regex could be matched to regex query: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html
                throw new Error("{} not yet implemented");
            }
            else if (typeof value == 'function')
            {
                throw new Error("value is a function!");
            }
            else
            {
                throw new Error("Unsupported value type/value: " + typeof value + "/" + value);
            }
        }
    }

    //obj = me.reduceSuperflousQueryShoulds(obj);
    return obj;
}

ElasticSearchEx.prototype.__reduceSuperflousQueryShoulds = function (me, obj)
{
    var reduced = false;
    var newObj = {};

    function reduceObject(a, obj)
    {
        var keys = Object.keys(a);
        if (keys.length > 0)
        {
            // get the first key - the rest is ignored anyhow..
            var k = Object.keys(a)[0];
            obj[k] = a[k];
            return true;
        }
        else
            return false;
    }

    function reduceBlock(bool, block, obj)
    {
        var value = bool[block];
        if (typeof value == 'object' && !(value instanceof Array)) // is's an object
        {
            return reduceObject(value, obj);
        }
        else if (typeof value == 'object' && bool.must instanceof Array) // it's an array
        {
            return reduceObject(value[0], obj);
        }
        else
            return false;
    }

    for (var key in obj)
    {
        if (key == 'bool')
        {
            var bool = obj[key];

            // does it have a should group with only one 1 item and a minimum_number_should_match = 1, and no "must" or "must_not" ?
            if (bool.should != null && bool.must == null && bool.must_not == null && bool.minimum_number_should_match == 1)
            {
                if (reduceBlock(bool, 'should', newObj))
                {
                    reduced = true;
                    continue;
                }
            }
            // or is it a lonely must with only 1 statement?
            else if (bool.should == null && bool.must_not == null && bool.must != null)
            {
                if (reduceBlock(bool, 'must', newObj))
                {
                    reduced = true;
                    continue;
                }
            }
        }

        // copy
        newObj[key] = obj[key];
    }
    return reduced ? newObj : obj;
}

ElasticSearchEx.prototype.__buildMVELExpression = function (me, conds)
{
    throw new Error("Not yet implemented");
    /*
    var obj = {};

    for (var key in conds)
    {
        if (key == '$or')
        {
            if (!obj.bool) obj.bool = {};
            if (!obj.bool.should) obj.bool.should = {};
            obj.bool.should.push(me.buildQuery(conds[key]));
            obj.minimum_should_match = 1;
        }
        else
        {
            var value = conds[key];
            if (typeof value == 'string' || typeof value == 'number')
            {
                if (obj['term'] == null) obj['term'] = {};
                obj.term[key] = value;
            }
            else if (typeof value == 'object')
            {
                // $gte, $lte, $lt, $gt could be matched to range queries: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
                // $regex could be matched to regex query: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html
                throw new Error("{} not yet implemented");
            }
        }
    }
    return obj;
    */
}


ElasticSearchEx.prototype.__deleteBy = function (me, options, conds, callback) // [options], conds, callback
{
    if (typeof conds == 'function' && callback == null)
    {
        callback = conds;
        conds = options;
        options = me.options;
    }

    var query = { query: me.buildQuery(conds) };
    me.client.deleteByQuery(options, query, callback);
}

ElasticSearchEx.prototype.__createIndex = function (me, options, data, callback)
{
    if (typeof data == 'function' && callback == null)
    {
        callback = data;
        data = options;
        options = me.options;
    }
    else
    {
        options = me.makeOptions(options);
    }

    me.client.indices.createIndex(options, data, callback);
}

/**
 * Does a mongoDB style $setOnInsert & $set combo upsert
 * (see http://docs.mongodb.org/manual/reference/operator/update/setOnInsert/)
 *
 * @param {es} The elasticsearch client instance
 * @param {id} Document id
 * @param {insert} Document to be inserted
 * @param {update} Document to be updated
 * @param {callback} Callback: function(err, insertResult, updateResult)
 */
ElasticSearchEx.prototype.__upsert = function (me, id, insert, update, callback)
{
    id = me.optionifyId(id);

    // First do the INSERT ONLY portion
    me.client.update(
        id,
        {
            'script': ' ', // required because elasticsearch.update() is weird
            'params': {},
            'upsert': insert
        },
        function (err, insertResult)
        {
            if (err)
            {
                if (callback) { callback(err, insertResult, null); return; }
                else throw err;
            }

            // Then do the UPSERT (insert & update) portion
            me.client.update(
                id,
                {
                    'doc': update,
                    'doc_as_upsert': true
                },
                function (err, updateResult)
                {
                    if (err && !callback) throw err;
                    if (callback) callback(err, insertResult, updateResult);
                }
            );
        }
    );
}

module.exports = ElasticSearchEx;