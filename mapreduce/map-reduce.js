/**
 * map reduce
 */

function Map() {
	emit(this.fid, {count : 1,time : this.time})
}

function Reduce(key, values) {
	var res = {count : 0,time : ""};
	res.time = values[0].time;
	for (var i = 0; i < values.length; i++) {
		res.count += values[i].count; // 计算总值
		if (res.time > values[i].time) {
			res.time = values[i].time; // 置于时间的最小值
		}
	}
	return res;
}

///~app request view
function Map() {
	emit(this.fid, {count : 1})
	}

function Reduce(key, values) {
	var res = {count : 0};

		for (var i = 0; i < values.length; i++) {
			res.count += values[i].count; // 计算总值
			}
		return res;
	}



function() {
	
	emit(this.fid,{count:1,request:this.request,
		                   push:this.push,
		                   view:this.view,
		                   click:this.click
		                   download:this.download
		                   install:this.install
		                   first:this.first})
}


function (key, values) {
	var res = {count : 0,request:0,push:0,view:0,click:0,download:0,install:0,first : ""};
	res.first = values[0].first;
	for (var i = 0; i < values.length; i++) {
		res.count += values[i].count;       // 计算总值
		res.request += values[i].request;     // 计算请求数
		res.push += values[i].push; 		// 计算推送数
		res.view += values[i].view; 		// 计算展示数
		res.click += values[i].click; 		// 计算点击数
		res.download += values[i].download; // 计算下载数
		res.install += values[i].install; 	// 计算安装数
		
		if (res.first > values[i].first) {
			res.first = values[i].first; // 置于时间的最小值
		}
	}
	return res;
}


function Map() {
	emit(
		this.day,				
		{count: 1, new_u:this.new_u, push:this.push,view:this.view,click:this.click,alive:this.alive}
	); 
	
}

function Reduce(key, values) {
	var res = {count : 0,new_u:0,push:0,view:0,click:0,alive:0};
	for (var i = 0; i < values.length; i++) {
		res.count += values[i].count;      
		res.new_u += values[i].new_u;     
		res.push += values[i].push; 	
		res.view += values[i].view; 	
		res.click += values[i].click; 	
		res.alive += values[i].alive; 
	}
	return res;
}







//点击数
function map()
{
	emit(this.fid, {count : 1,adid:this.adid,type:this.type, appid:this.appid})
}

function Reduce(key, values) {
	  var res = {count:0,cpc:0,wall:0,oth:0};
	  for (var i = 0; i < values.length; i++) {
	   res.count += values[i].count;
	   if(values[i].type == 1){
	     if(values[i].adid == values[i].appid) res.wall += values[i].count;
	     else res.oth += values[i].count;
	   }
	   else if(values[i].type == 2){
	     res.cpc += values[i].count;
	   }
	 }
	  
	  res.on_reduce = 1;
	 return res;
}


function Finalize(key, res) {
	if (res.on_reduce != 1) {
	res.cpc = 0;
	res.wall=0;
	res.oth=0;
	}
	return res;
	}



///---click
function Map() {
	 
	   var key = this.fid;
	   var value = {count:1,wall:0,oth:0,cpc:0};
	   if(this.type==2) value.cpc = 1;
	   if(this.type==1)
	   {
	      if(this.adid == this.appid) value.oth = 1;
		  else value.wall = 1;
	   }
	   
	   emit(key,value);
	}

function Reduce(key, values) {
	

	var res = {count:0,wall:0,oth:0,cpc:0};
	values.forEach(function(val) {
		res.count   += val.count; 	// reduce logic
		res.cpc += val.cpc; 
		res.wall += val.wall;
		res.oth += val.oth; 
	});
    res.on_reduce = 1;
	return res;	
}

function Finalize(key, reduced) {
	if (reduced.on_reduce != 1) {
		reduced.cpc = 0;
		reduced.wall=0;
		reduced.oth=0;
		}
		return reduced;
	}

////~  download

function Map() {
	   var key = this.fid;
	   var value = {count:1,wall:0,oth:0,time:this.time};
	   if(this.adid == this.appid) value.oth = 1;
	   else value.wall = 1;
	   emit(key,value);
	}


function Reduce(key, values) {
	var res = {count:0,wall:0,oth:0,time:0};
	res.time = values[0].time;
	values.forEach(function(val) {
		res.count   += val.count; 	
	    res.wall += val.wall;
		res.oth += val.oth; 
		if(val.time < res.time) res.time = val.time;
	});
    res.on_reduce = 1;
	return res;	
}


function Map() 
{
	if(this.imei!= null) var key = this.imei.brand;
	emit(key,{count: 1}); 
}

function Reduce(key, values) {
	var reduced = {count:0}; 
	values.forEach(function(val) {
		reduced.count += val.count; 
	});
	return reduced;	
}

//修改后
//click
function Map() {
	   var key = this.fid;
	   var value = {count:1,cpa:0,cpc:0,time:this.time};
	   if(this.type==1) value.cpa = 1;
	   if(this.type==2) value.cpc = 1;
	   emit(key,value);
	}

function Reduce(key, values) {
	var res = {count:0,cpa:0,cpc:0,time:''};
	res.time = values[0].time;
	values.forEach(function(val) {
		res.count   += val.count; 	
		res.cpa += val.cpa;
		res.cpc += val.cpc;
		if(val.time < res.time) res.time = val.time;
	});
 res.on_reduce = 1;
	return res;	
}
