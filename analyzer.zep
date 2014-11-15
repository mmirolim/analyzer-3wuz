/**
 * Exprimental rewrite of analytic script for www.uz
 * @link zephir-lang.org
 * @author Mirolim Mirzakhmedov
 * @email mirolim777 at gmail dot com
 * @date 28.07.13
 */
namespace Wwwuz;

class Analyzer
{
	// store batch queries to issue
	protected queries = [];
	/**
	 * dns as mysqli dns, table to process, batch size of processing records and thread id
	 * @todo refactor commit behaviour
	 */
    public final function doMagic(const array dns, const string table, const int batchSize, const string thread) -> void
    {
		string query;
		boolean commit;
		var check, q, link, result, record, records, provs;
		var ids = [] , pids = [], idatas = [], ips = [], paths = [], paths_real =[], agents = [], paths_ref = [], colors = [], monitors = [], vjss = [];

		let link = mysqli_connect(dns["host"], dns["user"], dns["pass"], dns["db"]);
		//link->set_charset("utf8");
		if !link {
			die("Error" . mysqli_error(link));
		}
		link->query("Update top_log.".table." SET thread = '".thread."', processed = 1 WHERE processed = 0 LIMIT ".batchSize);
		let query = "SELECT id, pid, idata, ip, path, path_real, agent, page_ref, color, monitor, vjs FROM top_log.".table." WHERE processed = 1 AND thread = '".thread."'";
		let result = link->query(query);
		let records = this->fetchData(result);
		// free result
		result->close();

		if !empty records {
			for record in records {
				let ids[] = record["id"];
				let pids[] = record["pid"];
		   	  	let idatas[] = link->real_escape_string(record["idata"]);
			    let ips[] = link->real_escape_string(record["ip"]);
			    let paths[] = link->real_escape_string(record["path"]);
	    	    let paths_real[] = link->real_escape_string(record["path_real"]);
		        let agents[] = link->real_escape_string(record["agent"]);
		   		let paths_ref[] = link->real_escape_string(record["page_ref"]);
		        let colors[] = link->real_escape_string(record["color"]);
				let monitors[] = link->real_escape_string(record["monitor"]);
				let vjss[] = link->real_escape_string(record["vjs"]);
			}
			let query = "SELECT DISTINCT topid as pid, ipaddress as ip, providerid as provid FROM catalog.resources WHERE topid IN (".implode(",", pids).")";
			let result = link->query(query);
			let provs = this->fetchData(result);

			// free result
			result->close();

			//this->addQueries(this->topBR(table, pids, agents));
			this->addQueries(this->topOS(table, pids, agents));
			this->addQueries(this->topPopPages(table, pids, paths));
	        this->addQueries(this->topVisitors(table, pids, ips, agents, colors, monitors, vjss, idatas));
	        this->addQueries(this->topHour(table, pids, ips, idatas));
	        this->addQueries(this->topProvHour(provs, table, pids, ips, idatas));
	        this->addQueries(this->topSpider(table, pids, agents));
			this->addQueries(this->topDomain(table, pids, paths, paths_real, paths_ref));
			

			let commit = true;
			echo "HERE";
			// disable autocommit
			link->autocommit(FALSE);
            int n = 0;
			for q in this->queries {
				let check = link->query(q);
				echo n."\n<br/>";
				//echo check."\n <br/> FOR QUERY ".q."\n<br/>";
                // 1 means var was true i guess
				if !check {
                    echo "I SHOULD NOT BE HERE<br/>";
                    //echo q."\n<br/>";
                    let commit = false;
                }
                let n++;
			}

			if commit {
				link->commit();
				link->query(" Update top_log.".table." SET processed=2 WHERE id in(".implode(",",ids).")");
			} else {
				link->rollback();
			}

		}
		// close connection
		link->close();
		
	}
	// get array from query result
	public final function fetchData(var result) -> array
	{
		var row, data = [];
		loop {
			let row = mysqli_fetch_array(result);
			if row {
				 let data [] = row;
			} else {
				break;
			}
		}
		return data;
	}
	// store batch queries
	public final function addQueries(const array query) -> void
	{
		var key, value;
		for key, value in query {
			let this->queries[] = value;
		}
	}
	// function to detect spider type
	public final function getSpider(var agent) -> string
	{
		var spider;
		var spiders = [
						["Aport", "Aport robot"],
						["Google", "Google"],
						["Yandex", "Yandex"],
						["Mail.Ru", "Mail.Ru Bot"],
						["www.uz", "WWW.UZ"],
						["msnbot", "MSN"],
						["Rambler", "Rambler"],
						["Yahoo", "Yahoo"],
						["AbachoBOT", "AbachoBOT"],
						["accoona", "Accoona"],
						["AcoiRobot", "AcoiRobot"],
						["ASPSeek", "ASPSeek"],
						["CrocCrawler", "CrocCrawler"],
						["Dumbot", "Dumbot"],
						["FAST-WebCrawler", "FAST-WebCrawler"],
						["GeonaBot", "GeonaBot"],
						["Gigabot", "Gigabot"],
						["Lycos", "Lycos spider"],
						["MSRBOT", "MSRBOT"],
						["Scooter", "Altavista robot"],
						["AltaVista", "Altavista robot"],
						["WebAlta", "WebAlta"],
						["IDBot", "ID-Search Bot"],
						["eStyle", "eStyle Bot"],
						["Scrubby", "Scrubby robot"],
						["YaDirectBot", "Yandex Direct"]
		];
		
		for spider in spiders {
			if stristr(agent, spider[0]) {
				if spider[0] == "www.uz" {
					if substr(strrev(agent),0,6) != "zu.www" && strlen(agent) == 6 {
						return spider[1];
					}
				} else {
					return spider[1];
				}
			}
		}
		return "";
	}
	// get user browser
	public final function getBrowser(string agent) -> array
	{
		 var browser_info = [], browser = [], version, opera = [], ie = [], ff = [];
		 preg_match("/(MSIE|Opera Mini|Opera Mobi|Opera|Firefox|Chrome|Version|Netscape|Konqueror|SeaMonkey|Camino|Minefield|Iceweasel|K-Meleon|Maxthon|UCWEB|Skyfire|NetFront)(?:[/]| )([0-9.]+)/", agent, browser_info);
		 let browser = browser_info[1];
		 let version = browser_info[2];
		 if preg_match("/Opera Mini/i", agent, opera) {
		 	return ["b":"Mobile Opera","v":"Mini"];
		 }
		 if preg_match("/Opera Mobi/i", agent, opera) {
		 	return ["b":"Mobile Opera","v":"Mobi"];
		 }
		 if preg_match("/NetFront/i", agent, opera) {
		 	return ["b":"Mobile NetFront","v":"Mobile"];
		 }
		 if preg_match("/BrowserNG/i", agent, opera) {
		 	return ["b":"Mobile BrowserNG","v":"Mobile"];
		 }
		 if preg_match("/Mobile Safari/i", agent, opera) {
		 	return ["b":"Mobile Safari","v":"Mobile"];
		 }
		 if preg_match("/UCWEB/i", agent, opera) {
		 	return ["b":"Mobile UCWEB","v":"Mobile"];
		 }
		 if preg_match("/Skyfire/i", agent, opera) {
		 	return ["b":"Mobile Skyfire","v":"Mobile"];
		 }
		 if preg_match("/Opera ([0-9.]+)/i", agent, opera) {
		 	return ["b":"Opera","v":opera[1]];
		 }
		 if (browser == "MSIE") {
			preg_match("/(Maxthon|Avant Browser|MyIE2)/i", agent, ie);
			if ie {
				return ["b" : ie[1]." based on IE", "v" : version];
			}
			return ["b":"IE","v" : version];
		  }
		 if (browser == "Firefox") {
		 	preg_match("/(Flock|Navigator|Epiphany)[/]([0-9.]+)/", agent, ff);
		  	if ff {
		  		return ["b" : ff[1], "v" : ff[2]];
		  	}
		 }
		 if browser == "Opera" && version == "9.80" {
		 	return ["b" : "Opera", "v" : substr(agent,-5)];
		 }
		 if browser == "Version" {
		 	return ["b" : "Safari", "v" : version];
		 }
		 if !browser && strpos(agent, "Gecko") {
		 	return ["b" : "Browser based on Gecko", "v" : ""];
		 }

		 return ["b" : browser, "v" : version];
	}

	// get user's OS type
	public final function getOs(string agent) -> string
	{
		var key, value;
	  	string opsys = "";
	  	var OSs = [
	  	  "FreeBSD" : "FreeBSD",
		  "OpenBSD" : "OpenBSD",
		  "Ubuntu" : "Ubuntu Linux",
		  "Debian" : "Debian Linux",
		  "Red Hat" : "Red Hat Linux",
		  "Mandriva" : "Mandriva Linux",
		  "Android" : "Android Mobile",
		  "Symbian" : "SymbianOS Mobile",
		  "Series 60" : "SymbianOS Mobile",
		  "iPad" : "iOS (iPad)",
		  "iPhone" : "iOS (iPhone)",
		  "SAMSUNG": "SAMSUNG Mobile",
		  "Windows Mobile": "Windows Mobile",
		  "Windows NT 5.1": "Windows XP",
		  "Windows NT 6.0": "Windows Vista",
		  "Windows NT 6.1": "Windows 7",
		  "Win" : "Windows",
		  "Linux": "Linux",
		  "Unix":"Unix",
		  "Mac": "Macintosh",
		  "OS/2":"OS/2",
		  "BeOS":"BeOS",
		  "J2ME/MIDP" : "Mobile",
		  "MIDP" : "Mobile"
	  			];

		for key, value in OSs {
			if strstr(agent, key) {
				let opsys .= value;
			}
		}
		if empty opsys {
			let opsys .= "OS undefined";
		}

		return opsys;

	}
	// get domain
	public final function getDomain(string path, string real_path, string page_ref) -> string
	{
		var k;
		string domain = "";
		if strcasecmp(path, real_path) != 0 {
			return "";
		}
		if strlen(path) < 11 {
			return "";
		}
		if strlen(page_ref) >= 11 {
			let k = parse_url(page_ref);
			let domain .= k["host"];
			return domain;
		}
		return "";
	}
	// get top spider
	public final function topSpider(string table, array pids, array agents) -> array
	{
		int i;
		var spider, md5;
		string v1 = "", v2 = "", q1, q2;
		let q1 = "INSERT INTO top_robot.".table." (pid,type,count) VALUES ";
   		let q2 = "INSERT INTO top_robot.".table."_list (name,md5) VALUES "; 

		for i in range (0, count(pids) - 1) {
			let spider = this->getSpider(agents[i]);
			if !empty spider {
				let md5 = md5(spider);
				let v1 .= "(".pids[i].",'".md5."',1),";
				let v2 .= "('".spider."','".md5."'),";
			}
		}
		let q1 = !empty v1 ? q1.substr_replace(v1,"",-1)." ON DUPLICATE KEY UPDATE count = count+1" : "";
		let q2 = !empty v2 ? q2.substr_replace(v2,"",-1)." ON DUPLICATE KEY UPDATE md5 = md5" : "";
		return [q1, q2];
	
	
	}
	// get top domain
	public final function topDomain(string table, array pids, array paths, array paths_real, array paths_ref) -> array
	{
		int i;
		var spider, domain, md5, md5ref, md5sp;
		string v1 = "", v2 = "", v3 = "", v4 = "", v5 = "", v6 = "", q1 = "", q2 = "", q3 = "", q4 = "", q5 = "", q6 = ""; 
		let q1 = " INSERT INTO top_domains.".table." (pid,type,count) VALUES "; // postDomen
		let q2 = " INSERT INTO top_domains.".table."_list (name,md5) VALUES "; // postDomen list
		let q3 = " INSERT INTO top_ref.".table." (pid,type,count) VALUES "; // postReferral
		let q4 = " INSERT INTO top_ref.".table."_list (name,md5) VALUES "; // postReferral list
		let q5 = " INSERT INTO top_search.".table." (pid,type,count) VALUES "; // postSearch
		let q6 = " INSERT INTO top_search.".table."_list (name,md5) VALUES "; // postSearch listlet 
		for i in range (0, count(pids) - 1) {
			let domain = strtolower(this->getDomain(paths[i], paths_real[i], paths_ref[i]));
			if !empty domain {
				let md5 = md5(domain);
				let md5ref = md5(paths_ref[i]);
	            let v1 .= "(".pids[i].",'".md5."',1),";
	            let v2 .= "('".domain."','".md5."'),";
	            let v3.="(".pids[i].",'".md5ref."',1),";
	            let v4.="('".paths_ref[i]."','".md5ref."'),";
	            let spider = this->getSpider(paths_ref[i]);
				if !empty spider {
	                let md5sp = md5(spider);
	                let v5.= "(".pids[i].",'".md5sp."',1),";
	                let v6 .= "('".spider."','".md5sp."'),";
	            }
			}
		}
		let q1 = !empty v1 ? q1.substr_replace(v1,"",-1)." ON DUPLICATE KEY UPDATE count=count+1" : "";
		let q2 = !empty v2 ? q2.substr_replace(v2,"",-1)." ON DUPLICATE KEY UPDATE md5=md5" :"";
		let q3 = !empty v3 ? q3.substr_replace(v3,"",-1)." ON DUPLICATE KEY UPDATE count=count+1" : "";
		let q4 = !empty v4 ? q4.substr_replace(v4,"",-1)." ON DUPLICATE KEY UPDATE md5=md5" :"";
		let q5 = !empty v5 ? q5.substr_replace(v5,"",-1)." ON DUPLICATE KEY UPDATE count=count+1" : "";
		let q6 = !empty v6 ? q6.substr_replace(v6,"",-1)." ON DUPLICATE KEY UPDATE md5=md5" : "";
		return [q1, q2, q3, q4, q5, q6];
	}
	// get top prov hour
	public final function topProvHour(var provs, string table, array pids, array ips, array idatas) -> array
	{
		int i;
		var kol, kol1, weektable, prov, yearMm, prIP, hour, _d;
		string v1 = "", v2 = "", q, q1, q2, q3; 
		let kol = date("w", strtotime(table));
		let kol1 = (kol == 0 ? 0 : 7 - kol) - 7;
		let weektable = date("Ymd", mktime(0, 0, 0, date("m",strtotime(table))  , (date("d", strtotime(table)) + kol1), date("Y", strtotime(table))));
		if !empty provs {
			let yearMm = date("Ym", strtotime(table));
			let q = " INSERT INTO top_prov_hour.".table." (prid,hour,count,ip) VALUES "; // postHour
			let q1 = " INSERT INTO top_prov_day.".table." (prid,ip,count) VALUES "; // postHour
			let q2 = " INSERT INTO top_prov_week.".weektable." (prid,ip,count) VALUES  "; // postHour
		    let q3 = " INSERT INTO top_prov_month.".yearMm." (prid,ip,count) VALUES "; // postHour

			for i in range(0, count(pids) - 1) {
	            let prIP = false;
	            for prov in provs {
					if prov["pid"] == pids[i] {
						let prIP = prov["ip"];
						break; // why one??
					}
			    }
			    // @todo check logic
				if !prIP || prIP == "0.0.0.0" || prIP == "255.255.255.255" || prIP == ""  {
					continue;
				} else {    
					let _d = explode(" ",date("h A", idatas[i]));
					let hour = _d[1] == "AM"  ?  ( (_d[0] + 12) == 24  ? 0 : _d[0] ) : ( (_d[0] + 12 == 24 ) ? 12 : _d[0] + 12 );
					let v1 .= "(".pids[i].",'".hour."',1,'".ips[i]."'),"; //pid, hour, count,ip
					let v2 .= "(".pids[i].",'".ips[i]."',1),"; //pid, ip, count
				}
			}
		let q = !empty v1 ? q.substr_replace(v1, "", -1)." ON DUPLICATE KEY UPDATE count=count+1" : "";
		let q1 = !empty v2 ? q1.substr_replace(v2, "", -1)." ON DUPLICATE KEY UPDATE count=count+1" : "";
		let q2 = !empty v2 ? q2.substr_replace(v2, "", -1)." ON DUPLICATE KEY UPDATE count=count+1" : "";
		let q3 = !empty v2 ? q3.substr_replace(v2, "", -1)." ON DUPLICATE KEY UPDATE count=count+1" : "";
		return [q,q1,q2,q3];

		}
		return [];
	}
	// get top hour
	public final function topHour(string table, array pids, array ips, array idatas) -> array
	{
		int i;
		var yearMm, kol, kol1, weektable, _d, hour;
		string v1 = "", v2 = "", q, q1, q2, q3; 
		let yearMm = date("Ym", strtotime(table));
		let kol = date("w", strtotime(table));
		let kol1 = (kol==0 ? 0: (7 - kol)) - 7;
		let weektable = date("Ymd",mktime(0, 0, 0, date("m",strtotime(table))  , (date("d",strtotime(table)) + kol1), date("Y",strtotime(table))));
		let q =" INSERT INTO top_hour.".table." (pid,hour,count,ip) VALUES "; // postHour
		let q1 =" INSERT INTO toprating.".table." (pid,ip,count) VALUES "; // postHour
		let q2 =" INSERT INTO top_week.".weektable." (pid,ip,count) VALUES  "; // postHour
		let q3 =" INSERT INTO top_month.".yearMm." (pid,ip,count) VALUES "; // postHour

		for i in range(0, count(pids) - 1) {
			let _d = explode(" ", date("h A", idatas[i]));
			let hour = _d[1] == "AM" ? ( (_d[0] + 12) == 24 ? 0: _d[0]) : ((_d[0] + 12) == 24 ? 12 : _d[0] + 12);
			let v1.= "(".pids[i].",'".hour."',1,'".ips[i]."'),"; //pid, hour, count,ip
			let v2.= "(".pids[i].",'".ips[i]."',1),"; //pid, ip, count
		}
		let q = !empty v1 ? q.substr_replace(v1, "",-1)." ON DUPLICATE KEY UPDATE count=count+1":"";
		let q1 = !empty v2 ? q1.substr_replace(v2, "",-1)." ON DUPLICATE KEY UPDATE count=count+1":"";
		let q2 = !empty v2 ? q2.substr_replace(v2, "",-1)." ON DUPLICATE KEY UPDATE count=count+1":"";
		let q3 = !empty v2 ? q3.substr_replace(v2, "",-1)." ON DUPLICATE KEY UPDATE count=count+1":"";
		return [q, q1, q2, q3];
	}

	public final function topVisitors(string table, array pids, array ips, array agents, array colors, array monitors, array vjss, array idatas) -> array
	{
		int i;
		var yearMm, kol, kol1, weektable, _d, hour, md5;
		string v1 = "", v2 = "", v3 = "", q, q1, q2, q3, q4, q5, q6; 
		let yearMm = date("Ym", strtotime(table));
		let kol = date("w",strtotime(table));
		let kol1 = ( kol == 0 ? 0 : 7 - kol) - 7;
		let weektable = date("Ymd",mktime(0, 0, 0, date("m",strtotime(table))  , (date("d",strtotime(table)) + kol1), date("Y",strtotime(table))));

		let q = " INSERT INTO top_visitor_hour.".table." (pid,hour,count,type) VALUES "; // postVisitors
		let q1 = " INSERT INTO top_visitor_day.".table." (pid,type,count) VALUES "; // postVisitors
		let q2 = " INSERT INTO top_visitor_week.".weektable." (pid,type,count) VALUES "; // postVisitors
		let q3 = " INSERT INTO top_visitor_month.".yearMm." (pid,type,count) VALUES "; // postVisitors
		let q4 = " INSERT INTO top_visitor_day.".table."_list (ip,agent,color,monitor,vjs,md5) VALUES "; // postVisitors list
		let q5 = " INSERT INTO top_visitor_week.".weektable."_list (ip,agent,color,monitor,vjs,md5) VALUES "; // postVisitors list
		let q6 = " INSERT INTO top_visitor_month.".yearMm."_list (ip,agent,color,monitor,vjs,md5) VALUES "; // postVisitors list

		for i in range(0, count(pids) - 1) {
			let md5 = md5(ips[i].agents[i].colors[i].monitors[i].vjss[i]);
			let _d = explode(" ",date("h A", idatas[i]));
			let hour = _d[1] == "AM" ? ((_d[0]+12) == 24 ? 0 : _d[0]) : ((_d[0] + 12) ==24 ? 12 : _d[0]+12);
			let v1 .= "(".pids[i].",'".md5."',1),"; //pid, type, count
			let v3 .= "(".pids[i].",'".hour."',1,'".md5."'),"; //pid, hour, count, type
			let v2 .= "('".ips[i]."','".agents[i]."','".colors[i]."','".monitors[i]."','".vjss[i]."','".md5."'),"; //pid, hour, type, count
		}
	    let q = !empty v3 ? q.substr_replace(v3,"", -1)." ON DUPLICATE KEY UPDATE count=count+1":"";
		let q1 = !empty v1 ? q1.substr_replace(v1, "", -1)." ON DUPLICATE KEY UPDATE count=count+1":"";
		let q2 = !empty v1 ? q2.substr_replace(v1, "", -1)." ON DUPLICATE KEY UPDATE count=count+1":"";
		let q3 = !empty v1 ? q3.substr_replace(v1, "", -1)." ON DUPLICATE KEY UPDATE count=count+1":"";
		let q4 = !empty v2 ? q4.substr_replace(v2, "", -1)." ON DUPLICATE KEY UPDATE md5=md5":"";
		let q5 = !empty v2 ? q5.substr_replace(v2, "", -1)." ON DUPLICATE KEY UPDATE md5=md5":"";
		let q6 = !empty v2 ? q6.substr_replace(v2, "", -1)." ON DUPLICATE KEY UPDATE md5=md5":"";
		return [q, q1, q2, q3, q4]; // q5, q6 ??
	}

	public final function topPopPages(string table, array pids, array paths) -> array
	{
		int i;
		var md5;
		string v1 = "", v2 = "", q, q1; 
		let q = " INSERT INTO top_pop.".table." (pid,type,count) VALUES "; // postPopPages
		let q1 = " INSERT INTO top_pop.".table."_list (name,md5) VALUES "; // postPopPages list
		for i in range(0, count(pids) - 1) {
			let md5 = md5(paths[i]);
			let v1 .= "(".pids[i].",'".md5."',1),";
			let v2 .= "('".addslashes(paths[i])."','".md5."'),";
		}
		let q = !empty v1 ? q.substr_replace(v1, "",-1)." ON DUPLICATE KEY UPDATE count=count+1" : "";
		let q1 = !empty v2 ? q1.substr_replace(v2, "",-1)." ON DUPLICATE KEY UPDATE md5=md5" : "";
		return [q, q1];
	}

	public final function topBR(string table, array pids, array agents) -> array
	{
		int i;
		var browser; var md5;
		string v1 = "", v2 = "", q, q1;
		let q = " INSERT INTO top_br.".table." (pid, type, count) VALUES ";
		let q1 = " INSERT INTO top_br.".table."_list (name, version, md5) VALUES ";

		for i in range(0, count(agents) - 1) {
			let browser = this->getBrowser(agents[i]);
			let md5 = md5(browser["b"].browser["v"]);
			let v1 .="(".pids[i].",'".md5."',1),";
			let v2 .="('".browser["b"]."','".browser["v"]."','".md5."'),";

		}
		let q = !empty v1 ? q.substr_replace(v1, "",-1)." ON DUPLICATE KEY UPDATE count=count+1" : "";
		let q1 = !empty v2 ? q1.substr_replace(v2, "",-1)." ON DUPLICATE KEY UPDATE md5=md5" : "";
		return [q, q1];
	}

	public final function topOs(string table, array pids, array agents) -> array
	{
		int i;
		var os, md5;
		string v1 = "", v2 = "", q, q1;
		let q =" INSERT INTO top_os.".table." (pid, type, count) VALUES ";
		let q1 =" INSERT INTO top_os.".table."_list (name, md5) VALUES ";
		for i in range(0, count(agents) - 1) {
			let os = this->getOs(agents[i]);
			let md5 = md5(os);
			let v1 .= "(".pids[i].",'".md5."',1),";
			let v2 .= "('".os."','".md5."'),";
		}
		let q = !empty v1 ? q.substr_replace(v1, "", -1)." ON DUPLICATE KEY UPDATE count=count+1" : "";
		let q1 = !empty v2 ? q1.substr_replace(v2, "", -1)." ON DUPLICATE KEY UPDATE md5=md5" : "";
		return [q, q1];
	}
		
}
