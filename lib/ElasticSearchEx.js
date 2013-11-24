var Util = require('util');

var requirex = require('./requirex');
var ClassUtil = requirex('class-util');
var Logger = requirex('logger');

function ElasticSearchEx(es, options, logger)
{
    var me = this;
    me.log = logger || new Logger.NullLogger();
    me.client = es;
    me.options = options || {};    
    ClassUtil.bindFunctions(this, true);
}

ElasticSearchEx.prototype.__ok = function (me, callback)
{
    if (typeof callback == 'function') callback(null);
    return undefined;
}

ElasticSearchEx.prototype.__fail = function (me, callback, error)
{
    if (typeof error == 'string') error = new Error(error);
    if (typeof callback == 'function') callback(error);
    return error;
}

ElasticSearchEx.prototype.__clone = function (me, obj)
{
    return JSON.parse(JSON.stringify(obj));
}

ElasticSearchEx.prototype.__makeOptions = function (me, moreOptions)
{
    var o = me.clone(me.options);
    if (moreOptions != null && typeof moreOptions == 'object')
        for (var i in moreOptions) o[i] = moreOptions[i];
    return o;
}

ElasticSearchEx.prototype.__select = function (me, options)
{
    return new me.constructor(me.client, me.makeOptions(options));
}

ElasticSearchEx.prototype.__optionifyId = function (me, id)
{
    return (id == null) ? me.makeOptions() : (typeof id != 'object') ? me.makeOptions({ _id: id }) : id;
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
        options = me.makeOptions();
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

ElasticSearchEx.prototype.__bulkInsert = function(me, options, docs, callback)
{
    if (callback == null && typeof docs == 'function')
    {
        callback = docs;
        docs = options;
        options = {};
    }
    options = me.makeOptions(options);
    me.client.bulkIndex(options, docs, callback);
}

ElasticSearchEx.prototype.__insert = function (me, id, doc, callback)
{
    if (callback == null && typeof doc == 'function')
    {
        callback = id;
        doc = id;
        id = null;
    }
    id = me.optionifyId(id);
    me.client.index(id, doc, callback);
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
            if (!(value instanceof Array)) throw new Error("$not must be matched against an array");
            var bool =
                {
                    'must_not': []
                }

            for (var i = 0; value.length > i; i++)
            {
                bool.must_not.push(me.buildQuery(value[i]));
            }
            obj.bool.must.push({ 'bool': bool });
        }
        else if (key == '$nor')
        {
            throw new Error("$nor not yet implemented"); // bool.should_not[bool.should[..]]  !(a|b|c)
        }
        else if (key == '$')
        {
            // let native functions pass through
            obj.bool.must.push(value);
        }
        else
        {
            var value = conds[key];
            if (typeof value == 'string' || typeof value == 'number' || typeof value == 'boolean' || value === null || value === undefined)
            {
                var o = {};
                o[key] = value;
                obj.bool.must.push({ term: o });
            }
            else if (typeof value == 'object')
            {
                // $where -> not possible, requires scripting
                // $mod   -> [ divisor, remainder ] = if field % divisor = remainder
                // $in    -> http://docs.mongodb.org/manual/reference/operator/query/in/
                // $nin   -> http://docs.mongodb.org/manual/reference/operator/query/nin/
                // $or    -> ??
                // $ne    -> bool.must_not ?
                // $gte, $lte, $lt, $gt could be matched to range queries: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
                // $regex could be matched to regex query: http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html
                var keys = Object.keys(value);
                if (keys.length != 1) throw new Error("Invalid number of keys in value object - must be 1: " + JSON.stringify(value));
                var op = keys[0];
                value = value[op];
                if (op == '$gte' || op == '$lte' || op == '$lt' || op == '$gt')
                {
                    op = op.substring(1);
                    var range = {};
                    range[key] = {};
                    range[key][op] = value;
                    obj.bool.must.push({ "range": range });
                }
                else throw new Error("{" + op + ": ..} not implemented");
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

    obj = me.reduceSuperflousQueryShoulds(obj);
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
        else if (typeof value == 'object' && value instanceof Array && value.length == 1) // it's an array
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
}

ElasticSearchEx.prototype.__deleteBy = function (me, options, conds, callback) // [options], conds, callback
{
    if (typeof conds == 'function' && callback == null)
    {
        callback = conds;
        conds = options;
        options = me.makeOptions();
    }

    var query = me.buildQuery(conds);
    //console.log("[delete]", "options:", JSON.stringify(options), "query:", JSON.stringify(query), "from conds:", JSON.stringify(conds));
    me.client.deleteByQuery(options, query, callback);
}

ElasticSearchEx.prototype.__createIndex = function (me, options, data, callback)
{
    if (typeof data == 'function' && callback == null)
    {
        callback = data;
        data = options;
        options = me.makeOptions();
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

ElasticSearchEx.prototype.__searchForEach = function (me, options, query, eachCallback, finalCallback)
{
    if (typeof query == 'function')
    {   
        finalCallback = eachCallback;     
        eachCallback = query;
        query = options;
        options = {};
    }
    options = me.makeOptions(options);
    query = ClassUtil.mergeObjects(me.clone(query), { size: 1 });

    me.searchAllPaged(options, query, function(res, next)
    {
        eachCallback(res, next);
    }, finalCallback);
}

ElasticSearchEx.prototype.__searchAll = function (me, options, query, callback)
{
    if (callback == null && typeof query == 'function')
    {
        callback = query;
        query = options;
        options = {};
    }
    options = me.makeOptions(options);

    var results = [];
    function append(res, next)
    {
        for (var i = 0; res.hits.hits.length > i; i++)
        {
            var s = res.hits.hits[i].fields || res.hits.hits[i]._source;
            results.push(s);
        }
        next();
    }
    me.searchAllPaged(options, query, append, function(err)
    {
        if (err) callback(err);
        else callback(null, results);
    });
}

ElasticSearchEx.prototype.__searchAllPaged = function (me, options, query, pageCallback, finalCallback)
{
    if (typeof query == 'function')
    {
        finalCallback = pageCallback;
        pageCallback = query;
        query = options;
        options = {};
    }
    options = me.makeOptions(options);

    var log = me.log;

    var size = query.size || 1000;
    var total = null;
    var remaining = null;
    var from = 0;
    var scrollId = null;

    function nextPage()
    {
        if (total == null || remaining > 0)
        {
            log.debug("Querying results: (from=" + from + ", size=" + size + ", total=" + total + ", remaining=" + remaining + ")");
            var q = ClassUtil.mergeObjects(me.clone(query), { from: from, size: size });
            if (scrollId != null) q.scroll_id = scrollId;
            me.client.search(options, q, function (err, res)
            {
                if (err)
                {
                    log.error("Error while querying results: ", err, err.stack);
                    return me.fail(finalCallback, err);
                }

                log.debug("Got " + res.hits.hits.length + " document(s)");
                if (total == null) // first result
                {
                    if (res.scroll_id != null) scrollId = res.scroll_id;
                    total = res.hits.total;
                    remaining = total;
                }
                else
                {
                    if (res.hits.total != total)
                    {
                        log.warning("Total changed while querying! (" + total + " -> " + res.hits.total + ")");
                    }
                }

                pageCallback(res, function()
                {
                    remaining -= res.hits.hits.length;
                    from += res.hits.hits.length;
                    if (res.hits.hits.length == 0)
                    {
                        if (remaining > 0)
                        {
                            log.warning("Incomplete result? Expected " + remaining + " more document(s)");
                        }
                        return me.ok(finalCallback); // may be partial
                    }

                    setImmediate(nextPage);
                });
            });
        }
        else
            return me.ok(finalCallback);
    }
    nextPage();
}

// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-request-scroll.html
// "Scrolling is not intended for real time user requests, it is intended for cases like scrolling over large portions of data that exists within elasticsearch to reindex it for example.


module.exports = ElasticSearchEx;