var hc=null;
var ts1=ts2=hChart={}
var ticker=MTtbl=MTfld=MTsqf=title=subtitle='';
var pbdate='20100101',freq='Q',analysis="template";
var scatterXY=false;

function excel2unix(t) {return( (t-25569)*86400000+18000000 );}
function Ymd2unix(t) {
	var y=parseInt(t/10000), d=t%100,m=(t-y*10000-d)/100-1;
	return( Date.UTC(y,m,d) );
}
function date2unix(t) {return( (t>12345678)?Ymd2unix(t):excel2unix(t) );}
function getUrlVars() {
	var vars = {};
	var urlapp = window.location.href.replace('#','');
	var parts = urlapp.replace(/[?&]+([^=&]+)=([^&]*)/gi,
		function(m,key,value) { vars[key] = value; });
	return vars;
}

function crtOpt_template(title="Time Series",subtitle="",ts,hctype="spline",addOpt) {
	var opts={};
	$.extend(opts,crtOpt_xAxis_general(title,subtitle,hctype), crtOpt_yAxis_series(ts,hctype));
	if(typeof(addOpt)!="undefined")
		opts=$.merge(addOpt,opts);
	return(opts)
}

function crtOpt_xAxis_general(title="Time Series",subtitle="",hctype="spline") {
	var xAxisOpt=(scatterXY)?
		{
			labels:{format:"{value}"}
		}
		:
		{
			type: 'datetime',
			dateTimeLabelFormats: { month: '%b \'%y' },
			title: { text: 'Date' }
		};
	opts= {
		chart: {
			alignTicks:true,
			zoomType:'xy',
			type: hctype
		},
		time: { useUTC: false },
		title: { text: title },
		subtitle: { text:subtitle },
		tooltip: { shared: true },
		legend: {
			layout: 'vertical', align: 'left', x: 100, 
			verticalAlign: 'top', y: 55, floating: true,
			backgroundColor: (Highcharts.theme && Highcharts.theme.legendBackgroundColor) || '#FFFFFF' },
		xAxis: xAxisOpt,
		plotOptions: {
			series: {
				events: {
					legendItemClick: function () {
						var ix=this.name,cbx=$("#"+ix);
						cbx.prop("checked", !cbx.prop("checked"));
						tgl_series(ix);
						return(false);
					}
				}
			},
			scatter: {
				marker: {
					radius: 3
				}
			}
		}
	}
	return(opts);
}

function crtOpt_yAxis_series(ts,hctype) {
	var opts={},yAxis=[],series=[];
	var xlst = Object.keys(ts);
	$.each(xlst,function(id,colname){
		yAxis.push(cr_yAxis_ele(colname,id));
		series.push(cr_series_ele(colname,id,hctype))
	});
	opts={yAxis,series};
	return(opts);
}

function cr_yAxis_ele(colname,id) {
	var yn=(id%2)?false:true;
	var xopt={labels: { format: '${value}' },opposite: true};
	xopt=$.merge({
		opposite: yn,
		title: { text: colname,
			style: { color: Highcharts.getOptions().colors[id] }
		}, label:{
			style: { color: Highcharts.getOptions().colors[id] }}},xopt);
	return(xopt);
}

function cr_series_ele(colname,id,type) {
	var ynLabel=(ts[colname].length>20)?false:true;
	var type=(ts[colname].length>30 || scatterXY)?type:'column';
	var xseries={
		yAxis: id,
		name: colname,
		type: type,
		marker: {enabled: scatterXY},
		//threshold:null,
		threshold:0,
		color: Highcharts.getOptions().colors[id],
		dataLabels: {enabled:ynLabel},
		data: ts[colname] }
	$("#"+colname+"_type").val(type); // update OptionMenu select options
	return(xseries);
}

function createChart(idName,chartOption) {
	chartOption.chart=$.merge({renderTo:idName},chartOption.chart);
	//var chart = new Highcharts.StockChart(chartOption);
	var chart = new Highcharts.Chart(chartOption);
	return chart;
}

function chtXYtemplate(idName,ticker,pbdate,freq,MTfld,MTtbl,analysis) {
	urlName='getMT'+analysis+'.pyx?'
		+ 'ticker='+ticker+'&pbdate='+pbdate+'&freq='+freq;
	if(MTsqf.length>0) {
		if(MTsqf.toLowerCase()=="na") {
			ts1={};
			return(false);
		}
		urlName= urlName
			+ '&sqlfile='+MTsqf;
	} else if(MTtbl.length*MTfld.length>0) {
		urlName= urlName
			+ '&field='+MTfld
			+ '&table='+MTtbl;
	}
	ts1={};
	$.getJSON(urlName,function(d) { 
		var xid=d.field.indexOf(ticker)+1;
		$.each(d.field,function(j,val){
			if(val!=ticker) 
				ts1[val]= $.map(d.data,function(x,i) {
					var y=[x[xid],x[j+1]]; return([y]); });
		});
	}); // end of getJSON function
}

function chtMTtemplate(idName,ticker,pbdate,freq,MTfld,MTtbl,analysis) {
	urlName='getMT'+analysis+'.pyx?'
		+ 'ticker='+ticker+'&pbdate='+pbdate+'&freq='+freq;
	if(MTsqf.length>0) {
		if(MTsqf.toLowerCase()=="na") {
			ts1={};
			return(false);
		}
		urlName= urlName
			+ '&sqlfile='+MTsqf;
	} else if(MTtbl.length*MTfld.length>0) {
		urlName= urlName
			+ '&field='+MTfld
			+ '&table='+MTtbl;
	}
	ts1={};
	$.getJSON(urlName,function(d) { 
		$.each(d.data,function(i,x) {
		x[0]=date2unix(x[0]); });

		$.each(d.field,function(j,val){
				ts1[val]= $.map(d.data,function(x,i) {
			var y=[x[0],x[j+1]]; return([y]); });
		});
	}); // end of getJSON function
}

function iexMTtemplate(idName,ticker,pbdate,freq,MTfld,MTtbl,analysis) {
	urlName='getMT'+analysis+'.pyx?'
		+ 'ticker='+ticker+'&pbdate='+pbdate+'&freq='+freq;
	if(MTsqf.length>0) {
		if(MTsqf.toLowerCase()=="na") {
			ts1={};
			return(false);
		}
		urlName= urlName
			+ '&sqlfile='+MTsqf;
	} else if(MTtbl.length*MTfld.length>0) {
		urlName= urlName
			+ '&field='+MTfld
			+ '&table='+MTtbl;
	}
	ts1={};
	$.getJSON(urlName,function(d) { 
		$.each(d.data,function(i,x) {
		x[0]=date2unix(x[0]); });

		$.each(d.field,function(j,val){
				ts1[val]= $.map(d.data,function(x,i) {
			var y=[x[0],x[j+1]]; return([y]); });
		});
	}); // end of getJSON function
}

function iexTStemplate(idName,ticker,pbdate,freq,analysis) {
	myObj = {"symbols":ticker,"types":"chart","range":"5y"};
	urlName=`/alan/alanapi/?search=hist&instrument=stock&ticker=${ticker}&topic=minute&output=json&api_key=e0e83fbe0893ee7a039c4f569083d0a1`;
	//urlName="https://api.iextrading.com/1.0/stock/market/batch";
	$.getJSON(urlName, myObj, function (datax) {
	ticker=myObj.symbols;
	dc = datax;
	// for price only
	data = dc.map(function(x,i) {
		v=[x.epochs ,x.close];
		return v;
	} );
	ts2={}
	ts2["price"]=data
	} );
}

function iex1TStemplate(idName,ticker,pbdate,freq,analysis) {
	myObj = {"symbols":ticker,"types":"chart","range":"5y"};
	urlName=`/alan/alanapi/?search=hist&instrument=stock&ticker=${ticker}&topic=minute&output=json&api_key=e0e83fbe0893ee7a039c4f569083d0a1`;
	//urlName="https://api.iextrading.com/1.0/stock/market/batch";
	$.getJSON(urlName, myObj, function (datax) {
	ticker=myObj.symbols;
	dc = datax;
	// for price only
	data = dc.map(function(x,i) {
		v=[x.epochs,x.close];
		return v;
	} );
	ts2={}
	ts2["price"]=data
	} );
}
	


function chtTStemplate(idName,ticker,pbdate,freq,analysis) {
	urlName='getTS'+analysis+'.pyx?'
		+ 'ticker='+ticker+'&pbdate='+pbdate+'&freq='+freq;
	ts2={};
	$.getJSON(urlName,function(d) {
		var xlst = Object.keys(d);
		$.each(d,function(key,val) {
			if(val.length>0) {
				$.each(val,function(i,x){x[0]=date2unix(x[0]);});
			} else {
				delete d[key];
			}
		});
		ts2=d;
	}); // end of getJSON function
}

function tgl_legend(e) {
	var hl=$(".highcharts-legend"); hl.toggle(); 
	$("#"+e.id).css("background",(hl.css("display")=="none")?"#aaa":"#eee");
}

function tgl_series(idName) {
	var jqs=$('#'+idName);
	if(jqs.length<1) return(false);
	var js=parseInt(jqs.attr("series")),
		jy=parseInt(jqs.attr("y"));
	//var yn=(jqs.attr("checked")=="checked")?true:false;
	//var ttlyn=(jqs.attr("checked")=="checked")?idName:null;
	var yn=jqs[0].checked, ttlyn=(yn)?idName:null;
	hc.series[js].setVisible(yn);
	hc.yAxis[jy].update({labels:{enabled:yn},title:{text:ttlyn}});
}

function chg_hctype(idName) {
	var jqs=$('#'+idName);
	if(jqs.length<1) return(false);
	var js=parseInt(jqs.attr("series")),
	 jt=$("#"+idName+" :selected").val(),
	 jtyp={
		"line":{type:"line",lineWidth:2,dashStyle:"solid",marker:{enabled:false}},
		"column":{type:"column",pointWidth:10},
		"scatter":{type:"scatter",zoomType:'xy',marker:{enabled:true,radius:5},tooltip:{valueDecimals:2}},
		"area":{type:"area"},
		"spline":{type:"spline",lineWidth:2,dashStyle:"solid",marker:{enabled:false}},
		"splineMarker":{type:"spline",lineWidth:2,dashStyle:"solid",marker:{enabled:true}},
		"markerOnly":{type:"line",lineWidth:0,marker:{enabled:true,radius:3},tooltip:{valueDecimals:2}},
		"splineDot":{type:"spline",lineWidth:2,dashStyle:"Dot"},
		"splineDash":{type:"spline",lineWidth:2,dashStyle:"Dash"},
		"columnThin":{type:"column",pointWidth:1},
		"columnThick":{type:"column",pointWidth:20}};
	hc.series[js].update(jtyp[jt]);
}

function add_inChk(ts) {
	var xlst = Object.keys(ts);
	var txt='';
	$.each(xlst,function(id,colname){
	txt = txt+'<input type="checkbox" id="'
	   +colname+'" series='+id+' y='+id+' checked>';
	txt = txt+'<select id="'
	   +colname+'_type" series='+id+' y='+id+' >';
	txt = txt+'<option value=line selected >line</option>';
	txt = txt+'<option value=column	  >column</option>';
	txt = txt+'<option value=spline	  >spline</option>';
	txt = txt+'<option value=scatter >scatter</option>';
	txt = txt+'<option value=area >area</option>';
	txt = txt+'<option value=splineMarker   >splineMarker</option>';
	txt = txt+'<option value=markerOnly   >markerOnly</option>';
	txt = txt+'<option value=splineDot   >splineDot</option>';
	txt = txt+'<option value=splineDash  >splineDash</option>';
	txt = txt+'<option value=columnThin  >columnThin</option>';
	txt = txt+'<option value=columnThick >columnThick</option>';
	txt = txt+'</select>';
	txt = txt+'&nbsp;&nbsp;';
	txt = txt+colname;
	txt = txt+'<BR>';
	});
	$("#inChk").empty();
	$("#inChk").append(txt);
	$("select").css({"color":"gray"});
	// Create checkbox & select event listeners
	$(":checkbox").click(function(e){
	tgl_series(e.currentTarget.id);
	});
	$("select[id$=_type]").change(function(e){
	chg_hctype(e.currentTarget.id);
	});

}

function hcTSplot(idName,ticker,pbdate,freq,field,table,title,subtitle,analysis,callback) {
	if(scatterXY) {
		iexXYtemplate(idName,ticker,pbdate,freq,field,table,analysis);
	} else {
		iexMTtemplate(idName,ticker,pbdate,freq,field,table,analysis);
		iexTStemplate(idName,ticker,pbdate,freq,analysis);
	}
	$(document).ajaxStop(function(){
		$('#ajaxBusy').hide();
		updTSplot(idName,ticker,pbdate,freq,field,table,title,subtitle,analysis,callback);
	});
}

function updTSplot(idName,ticker,pbdate,freq,field,table,title,subtitle,analysis,callback) {
	ts=$.extend(ts2,ts1);
	add_inChk(ts);
	hctype=(scatterXY)?'scatter':'spline';
	hc_opts=crtOpt_template(title,subtitle,ts,hctype);
	hc=createChart(idName,hc_opts);
	//$('#container').highcharts(hc_opts);
	if(callback) {
		callback(); // run custom_{analysis}
	}
}

function plotTS(idName,ts,title="Time Series",subtitle="",hctype="spline",callback) {
	add_inChk(ts);
	hc_opts = crtOpt_template(title,subtitle,ts,hctype);
	hc = createChart(idName,hc_opts);
	if(callback==null || typeof(callback)=="undefined") callback=custom_default;
	callback();
	return(hc)
}

// custom Setup for templateAnalysis
function custom_template() {
	var ix;
        $.each($(":checkbox"),function(j,x){
                var ix=x.id,
                px=$("#"+ix);
                if(j!=2&&j!=4&&j!=5&&j<12) {
                        px.prop("checked",false);tgl_series(ix);
                }
        });
	ix='marketcap'+'_type';$("#"+ix).val("splineDot").change;chg_hctype(ix);
}

// custom default Setup 
function custom_default() {
	var ix;
	$.each($(":checkbox"),function(j,x){
		var ix=x.id,
		px=$("#"+ix);
		if(j>1) {
			px.prop("checked",false);tgl_series(ix);
		}
	});
}

function init_analysis(callback) {
	Highcharts.setOptions({
		exporting:{chartOptions:{chart:{width:1200,height:800}}},
		colors:['#50B432','PINK','RED','#24CBE5','#64E572','#FF9655','#FFF263'
		,'#6AF9C4','#EAEAEA','#666999','#006699','salmon','#DDDF00','LIGHTBLUE','PURPLE'] }); 
	if(callback==null || typeof(callback)=="undefined") callback=custom_default;
	analysis=(typeof param.analysis=="undefined")?analysis:param.analysis;
	ticker=(typeof param.ticker=="undefined")?ticker:param.ticker.toUpperCase();
	pbdate=(typeof param.pbdate=="undefined")?pbdate:param.pbdate;
	freq=(typeof param.freq=="undefined")?freq:param.freq;
	MTtbl=(typeof param.table=="undefined")?MTtbl:param.table;
	MTfld=(typeof param.field=="undefined")?MTfld:param.field;
	MTsqf=(typeof param.sqlfile=="undefined")?MTsqf:param.sqlfile;
	subtitle=(typeof param.subtitle=="undefined")?subtitle:param.subtitle;

	$("#ticker").val(ticker);
	if(/^(\d){8}$/.test(pbdate)) 
		$('#from').val(pbdate.substr(0,4)+'-'+pbdate.substr(4,2)+'-'+pbdate.substr(6,2));

	titleDsr=analysis+' '+ ((scatterXY)?'Correlation':'Analysis');
	title=(typeof param.title=="undefined")?titleDsr: decodeURIComponent(param.title);
	hcTSplot('container',ticker,pbdate,freq,MTfld,MTtbl,title,subtitle,analysis,callback);

	//setup ticker/from/freq listener events
	$("[id=ticker],[id=from],[id=freq]").change(function(e){
		$("#updBtn").css("background","red");
	});
	$("#ticker").on( "autocompletechange", function(event,ui) {
		$("#updBtn").css("background","red");
	});
	$("#updBtn").click(function(){
		$("#updBtn").css("background","#eee");
		subtitle=ticker = $("#ticker").val();
		if( $("#from").val().length<10) $("#from").val()="2014-12-31";
		pbdate= $("#from").val().replace(/-/g,"");
		freq = $('select#freq option:selected').val();
		hcTSplot('container',ticker,pbdate,freq,MTfld,MTtbl,title,subtitle,analysis,callback);
		$(":checkbox").attr("checked","checked");
	});
	
	$.ajax({url:"tickers.json?callback=",contentType:"application/json",
		success:function(d){
			tks=d; 
			if(typeof($("#ticker").autocomplete)!="undefined")
			$("#ticker").autocomplete({source:tks}); 
		}
	});

	// Ajax activity indicator bound to ajax start/stop document events
	$(document).ajaxStart(function(){
		// $.blockUI({msg:$('#ajaxBusy'),css{width:'100%',height:'100%'}}).ajaxStop(function(){$.unblockUI();});
		$('#ajaxBusy').show(); }).ajaxStop(function(){ $('#ajaxBusy').hide();
	});

}
