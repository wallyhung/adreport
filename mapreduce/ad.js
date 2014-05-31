/**
 * map reduce
 */




//点击数
function map()
{
	emit(this.fid, {count : 1,adid:this.adid,type:this.type, appid:this.appid})
}

function reduce(key, values) {
	var res = {count:0,cpc : 0,wall:0,oth:0};
	for (var i = 0; i < values.length; i++) {
		res.count += values[i].count; // 计算总值
		if(values[i].type == 1)   //cpa点击
		{
			if(values[i].adid == values[i].appid) res.wall += values[i].count;
			else res.oth += values[i].count;
		}
		
		else if(values[i].type == 2) //cpc点击
		{
			res.cpc += values[i].count;
		}
	}
	return res;
}

function Reduce(key, values) {
	 var res = {count:0,wall:0,oth:0};
		for (var i = 0; i < values.length; i++) {
			res.count += values[i].count; // 计算总值
			if(values[i].adid == values[i].appid) res.wall += values[i].count;
			else res.oth += values[i].count;		
		}
		return res;
	}





function Reduce(key, values) {
	var res = {count:0,cpa:0,cpc:0};
	values.forEach(function(val) {
		res.count   += val.count; 	
		res.cpa += val.cpa;
		res.cpc += val.cpc; 
	});
res.on_reduce = 1;
	return res;	
}




// appday
function Map() 
{
	   var key = this.fid;
	   var value = {count: 1, request: this.request, push:this.push, view: this.view, c_cpc: 0, c_cpa: 0,d_wall: 0, d_oth: 0, i_wall: 0, i_oth: 0, first: this.first};
	   if(this.click != null)
	   {
		   value.c_cpc = this.click.cpc;
		   value.c_cpa = this.click.wall;
	   }
	   if(this.download != null)
	   {
		   value.d_wall = this.download.wall;
		   value.d_oth = this.download.oth;
	   }
	   if(this.install != null)
	   {
		   value.i_wall = this.install.wall;
		   value.i_oth = this.install.oth;
	   }
	   emit(key,value);
}

function Reduce(key, values) {
	var res = {count:0, request:0, push:0, view:0, c_cpc:0, c_cpa:0, d_wall:0, d_oth:0, i_wall:0, i_oth:0, first:''};
	    res.first = values[0].first;
	    for (var i = 0; i < values.length; i++) {
	        res.count += values[i].count;
	        res.request += values[i].request;
	        res.push += values[i].push;
			res.view += values[i].view;
			res.c_cpc += values[i].c_cpc;
	        res.c_cpa += values[i].c_cpa;
	        
	        res.d_wall += values[i].d_wall;
	        res.d_oth += values[i].d_oth;
	        
			res.i_wall += values[i].i_wall;
	        res.i_oth += values[i].i_oth;
	        
			if (res.first > values[i].first){
	            res.first = values[i].first;
	        }
	    }
	    return res;
	}


//adday
function() {
    emit(this.adid, {
        count: 1,
        push: this.push,
        view: this.view,
        click: this.click,
        download: this.download,
        install: this.install
    })
}

function(key, values) {
    var res = {
        count: 0,
        push: 0,
        view: 0,
        click: 0,
        download: 0,
        install: 0
    };
    for (var i = 0; i < values.length; i++) {
        res.count += values[i].count;
        res.push += values[i].push;
        res.view += values[i].view;
        res.click += values[i].click;
        res.download += values[i].download;
        res.install += values[i].install;
    }
    return res;
}






///______ad sum

function() {
	  emit(this.day, {
	        count: 1,
	        push: this.push,
	        view: this.view,
	        click: this.click,		
	        alive: this.alive
	    });
}
function(key, values) {
	 var res = {
		        count: 0,
				push:0,
		        view: 0,
				click:0,	
				alive:0
		    };
		    for (var i = 0; i < values.length; i++) {
		        res.count += values[i].count;
		        res.push += values[i].push;
		        res.view += values[i].view;
		        res.click += values[i].click;
			    res.alive += values[i].alive;
		    }
		    return res;
}


