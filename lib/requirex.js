var FS = require('fs');

var isWindows = (process.platform == 'win32');
var pathDelim = isWindows ? ';' : ':';
var dirDelim = isWindows ? '\\' : '/';

function find(lib)
{
    var nodePaths = (process.env['NODE_PATH'] || "").split(pathDelim);
    try
    {
        for (var i in nodePaths)
        {
            var p = nodePaths[i] + dirDelim + lib;
            if (FS.existsSync(p + dirDelim + 'package.json')) return p; // is there a <path>\<lib>\package.json ?
        }
    }
    catch (e) { }
    return null;
}

function requirex(lib)
{
    try
    {
        return require(lib);
    }
    catch (e)
    {
        if (e.code == 'MODULE_NOT_FOUND')
        {
           var p = find(lib);
           if (!p) p = find('node-' + lib);
           if (!p) throw e;
           return require(p);
        }
        else
            throw e;
    }
}

module.exports = requirex;