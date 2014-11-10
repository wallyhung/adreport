/**
 * connect to mongo
 */

var connection = function()
{
	db = connect("183.61.162.45:10000");
	return db;
};