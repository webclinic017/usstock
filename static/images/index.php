<html>
<head>
<script type="text/javascript" src="/scripts/jquery.min.js"></script>
</head>
<style>
#smallBody {
        float: left;
        overflow: hidden;
        text-align: center;
        height: 250;
	font-size: 8pt;
}
img {max-height:100px;max-width:100px}
</style>
<?PHP
$xout='';
$drX="./";
if(isset($_GET['Dir'])) $dirX= $_GET['Dir'];
$fps=explode("\n",`ls {$dirX}`);
$finfo = finfo_open(FILEINFO_MIME_TYPE); // return mime type ala mimetype extension
for($j=0;$j<count($fps);$j++) { 
	 $filename=$fps[$j];
	if(is_dir($filename))  {
		echo "DIRECTORY: <a id=dirLink href=\"$filename\" >$filename</a>";
		array_splice($fps,$j,1); 
	}
}
echo "<BR><BR>";
$jk=0;
foreach($fps as $filename) {
	list($type,$ext)=explode("/",@finfo_file($finfo, $filename));
	if ($type=="image") {
//		echo $type;
		if(filesize($filename)>5000) {
			echo "<a id=bigLink target=NEW href=\"$filename\" >  $filename</a>";
		} else {
			echo "<a id=smallLink target=NEW href=\"$filename\" > $filename</a>";
		}
		$jk++;
		if($jk & 1) { echo "<BR>"; }
	}
}
echo $xout;
finfo_close($finfo);
?>
<script>
$("a[id$=Link]").html(function(i,d){return '<img src="'+d+'" >'+'&nbsp; : &nbsp;'+d;});
</script>
