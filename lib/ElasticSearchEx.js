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