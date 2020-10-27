// itree ver 1.2.2
// 2012-10-29
// by Hossein Rahmani

var itree=(function(){
	var option={
		icons:["images/folder.gif","images/document.png"],
		images:["images/plusik.gif","images/plusik_rtl.gif","images/plusik_l.gif","images/plusik_l_rtl.gif","images/minus.gif","images/minus_rtl.gif","images/minus_l.gif","images/minus_l_rtl.gif","images/hr.gif","images/hr_rtl.gif","images/hr_l.gif","images/hr_l_rtl.gif","images/vertical.gif","images/vertical_rtl.gif"],
		ltotal:0,
		isrtl:false,
		loadondemand:false,
		leafnodetype:1,
		open:function(){
			
		},
		select:function(o){
			//console.log("selected id=" + o.id + " title=" + o.title );
			alert("selected");
		},
		lod:function(id){
			return " ";
		}
		
	}
	var cOption=function(d){
		//console.dir($(d).parents(".itree").data("options"));
		var c=$(d).parents(".itree").data("options");
		var a=jQuery.extend(true, {}, itree.option);
		$.extend(a,c);
		return a;
	}
	
	var plusik=function(il,op){
		return (il?(op.isrtl?op.images[3]:op.images[2]):(op.isrtl?op.images[1]:op.images[0]));
	}
	
	var minus=function(il,op){
		return (il?(op.isrtl?op.images[7]:op.images[6]):(op.isrtl?op.images[5]:op.images[4]));
	}
	
	var hr=function(il,op){
		return (il?(op.isrtl?"'"+op.images[11]+"'":"'"+op.images[10]+"'"):(op.isrtl?"'"+op.images[9]+"'":"'"+op.images[8]+"'"));
	}
	
	var vertical=function(op){
		return (op.isrtl?op.images[13]:op.images[12]);
	}
	
	
	
			var ievents={
					"select":function(id,itype,d){
						console.log("SelectedItem id=" + id + " type=" + itype );
						$(d).parents(".itree").find(".selectedNode").removeClass("selectedNode");
						$(d).addClass("selectedNode");
						$(d).parents(".itree").attr("data-selectednode","{\"id\":"+id+",\"nodetype\":"+itype+",\"title\":\""+$(d).html()+"\"}");
						console.log($(d).parents(".itree").attr("data-selectednode"));
						var co=itree.cOption(d);
						var at= $(d).parents(".itree").attr("data-selectednode");
						var obj=jQuery.parseJSON(at);
						co.select(obj);
					},
					"open":function(d,il){
						var childtr=$(d).parents(".trnode").next(".trchilds");
						$(childtr).toggle();
						var co=itree.cOption(d);
						if($(childtr).css("display")=="none"){
							var ic=plusik(il,co);
						}else{
							var ic=minus(il,co);
						}
				
						$(d).attr("src",ic);
						
						//load on demand
						console.log("check if lod");
						if($(childtr).hasClass("lod")){
							console.log("lod is called");
							itree.lod($(childtr));
						}
						
						//call open function
						co.open();
					}
			};
	
			
			var selectedNode=function(con){
				var at= con.attr("data-selectednode");
				var obj=jQuery.parseJSON(at);
				console.dir(obj);
				return obj;
			}
			
			
		var fillTree= function (n,l,ppp,op,res){
				
				if(n.length>0){
					res +="<table cellpadding='0' cellspacing='0'>";
					for (var i=0;i<$(n).length;i++){
						var pp=[];
						for(var tt=0;tt<ppp.length;tt++)
						{
							pp[tt]=ppp[tt];
						}
						
						res +="<tr class='trnode'><td><table cellpadding='0' cellspacing='0'><tr><td>";
						for (var p=0;p<l;p++){
	
							res +="<img src='" + ((pp[p]==true || pp[p]=="true") ?"images/vertical_b.gif" :vertical(op)) + "' />";
						}
						var il=(i+1) ==$(n).length;
						pp[l]=il;
						var childcount=n[i].childs ? n[i].childs.length:0;
						res +="<img  src="+ ((op.loadondemand && (childcount==0) && n[i].type != op.leafnodetype)?(plusik(il,op) +" style='cursor:pointer;' onclick='itree.ievents.open(this,"+ il +");' "):(((childcount>0)) ? ((l<op.ltotal || op.ltotal==-1)? minus(il,op):plusik(il,op)) +" style='cursor:pointer;' onclick='itree.ievents.open(this,"+ il +");' " : hr(il,op) )) + "/>";
					
						res +="<img src='"+op.icons[n[i].type] +"' /></td><td onclick='itree.ievents.select("+ n[i].id +","+ n[i].type +",this);' style='cursor:pointer;padding-right:5px;'>"+n[i].title+"</td></tr></table></td></tr>";
						
						if(childcount>0){
							res +="<tr class='trchilds' style='display:"+ ((l<op.ltotal || op.ltotal==-1)?"block":"none") + ";' ><td>";
							res =fillTree(n[i].childs,l+1,pp,op,res);
							res +="</td></tr>";
						}else if(op.loadondemand){
							res +="<tr class='trchilds lod' style='display:none;' data-parentid='"+ n[i].id +"' data-l='"+ l +"' data-pp='"+ pp +"' ><td>";
							res +="</td></tr>";
						}
					
					}
					res +="</table>";
				}
				return res;
			}
			
			var lod=function(con){
				var parentid= con.attr("data-parentid");
				var op=itree.cOption(con);
				op.lod(parentid,con);
			}
			
			var afterlod=function(con,n){
				console.log("load on demand...");
				var parentid= con.attr("data-parentid");
				var l=con.attr("data-l");
				var pp=con.attr("data-pp").split(",");
				console.log("load on demand pp=" + pp);
				console.log("load on demand pp=" + pp.length);
				var op=itree.cOption(con);
				
				var re="";
				re=fillTree(n,(l*1)+1,pp,op,re);
				con.find("td").html(re);
				con.removeClass("lod");
			}
			
			var fill=function(con,nodes,options){
				re="";
				
				var a=jQuery.extend(true, {}, itree.option);
				$.extend(a,options);
				re=fillTree(nodes,0,[],a,re);
				con.html(re);
				con.data("options",a);
			}
			return {
				fill:fill,
				ievents:ievents,
				selectedNode:selectedNode,
				cOption:cOption,
				lod:lod,
				option:option,
				afterlod:afterlod
			}
	
}());
