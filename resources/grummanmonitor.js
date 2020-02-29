'use strict';
/**
 *
 */

(function() {


var appCommand = angular.module('grummanmonitor', ['googlechart', 'ui.bootstrap','ngSanitize', 'ngModal', 'ngMaterial']);


/* Material : for the autocomplete
 * need 
  <script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.5.5/angular.min.js"></script>
  <script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.5.5/angular-animate.min.js"></script>
  <script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.5.5/angular-aria.min.js"></script>
  <script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.5.5/angular-messages.min.js"></script>

  <!-- Angular Material Library -->
  <script src="https://ajax.googleapis.com/ajax/libs/angular_material/1.1.0/angular-material.min.js">
 */



// --------------------------------------------------------------------------
//
// Controler grumman
//
// --------------------------------------------------------------------------

// grumman the server
appCommand.controller('GrummanControler',
	function ( $http, $scope,$sce,$filter ) {

	this.listevents='';
	this.inprogress=false;
	this.navbaractiv = 'synthesis';
	this.navbaractiv = 'reconciliationmsg';
	
	
	
	this.getNavClass = function( tabtodisplay )
	{
		if (this.navbaractiv === tabtodisplay)
			return 'ng-isolate-scope active';
		return 'ng-isolate-scope';
	}

	this.getNavStyle = function( tabtodisplay )
	{
		if (this.navbaractiv === tabtodisplay)
			return 'border: 1px solid #c2c2c2;border-bottom-color: transparent;';
		return 'background-color:#cbcbcb';
	}
	
	
	// -----------------------------------------------------------------------------------------
	//  									General tool
	// -----------------------------------------------------------------------------------------


	this.selectMessage = function( selection, listmessages ) 
	{
		console.log("selectMessage="+selection+" length="+listmessages.length);
		for (var i in listmessages) {
			// console.log(" set message "+i+" >"+i);
			var message=listmessages[ i ];
			if (selection === 'all' && message.status !== 'complete')
				message.selected = true;
			if (selection === 'none')
				message.selected = false;
			if (selection === 'incomplete')
				message.selected = message.status==='incompletecontent';
			if (selection === 'complete')
				message.selected = message.status==='complete';
		}
	}
	this.showDetailMessage = function( selection, listmessages ) 
	{
		console.log("showDetailMessage="+selection+" length="+listmessages.length);
		for (var i in listmessages) {
			// console.log(" set message "+i+" >"+i);
			var message= listmessages[ i ];
			message.showdetail = selection;
		}
	}
	
	this.getDetailBackground = function( detail ) {
		if (detail.statusexec === "sended" || detail.statusexec === "sendedandpurge" || detail.statusexec === "purged")
			return "label label-success";
		if (detail.statusexec === "sendfailed")
			return "label label-danger"; 
		if (detail.statusexec === "duplicate")
			return "label label-warning";
		return "";
	}
	

	// -----------------------------------------------------------------------------------------
	//  									Synthesis
	// -----------------------------------------------------------------------------------------

	
	this.getSynthesis = function()
	{
		var self=this;
		self.inprogress=true;
		// 7.6 : the server force a cache on all URL, so to bypass the cache, then create a different URL
		var d = new Date();
		
		$http.get( '?page=custompage_grumman&action=synthesis&t='+d.getTime() )
				.success( function ( jsonResult, statusHttp, headers, config ) {
					// connection is lost ?
					if (statusHttp==401 || typeof jsonResult === 'string') {
						console.log("Redirected to the login page !");
						window.location.reload();
					}
					console.log("history",jsonResult);
					self.synthesis 		= jsonResult;
					self.inprogress=false;
						
						
				})
				.error( function() {
					self.inprogress=false;
					});
				
	}
	// -----------------------------------------------------------------------------------------
	//  									Incomplete Message
	// -----------------------------------------------------------------------------------------

	this.reconciliationmsg = { 'showdetailexecution':'hide', 
			'detection': {}, 
			'numberofmessages': 500, 
			'purgeAllRelatives': true, 
			'sendincomplete' : true,
			'executecomplete' : true};
	
	this.getIncompleteReconciliationMessage = function()
	{
		console.log("start incompletereconciliationmessage");
		var self=this;
		self.inprogress=true;
		this.reconciliationmsg.showresult=false;

		// 7.6 : the server force a cache on all URL, so to bypass the cache, then create a different URL
		var d = new Date();
		
		$http.get( '?page=custompage_grumman&action=incompletereconciliationmessage&numberofmessages='+this.reconciliationmsg.numberofmessages+'&t='+d.getTime() )
				.success( function ( jsonResult, statusHttp, headers, config ) {
					// connection is lost ?
					if (statusHttp==401 || typeof jsonResult === 'string') {
						console.log("Redirected to the login page !");
						window.location.reload();
					}
					console.log("incompletereconciliationmessage",jsonResult);
					self.reconciliationmsg.detection 		= jsonResult;
					self.selectMessage( 'incomplete', self.reconciliationmsg.detection.listmessages );
					self.showDetailMessage( 'hide', self.reconciliationmsg.detection.listmessages);
					self.inprogress=false;
				})
				.error( function() {
					self.inprogress=false;
					});
				
	}
	
	// temporaire direct call to get reconciliation message
	// this.getIncompleteReconciliationMessage();
	
	
	this.getReconciliationBackground = function( message ) {
		if (message.status === "incompletecontent")
			return "background-color:#eae4ff"; // pink
		if (message.status === "faileddesign")
			return "background-color:#fe7171"; // red
		if (message.status === "complete")
			return "background-color:#eff397"; // Yellow
		
		return "background-color:#ecf0f1" // gray
	}
	
	
	
	
	
	
	this.sendIncompleteReconciliationMessage= function () {
		var list= [];
		
		for (var i in this.reconciliationmsg.detection.listmessages) {
			if (this.reconciliationmsg.detection.listmessages[ i ].selected)
				list.push( this.reconciliationmsg.detection.listmessages[ i ].keygroup );
		}
		console.log("listKeyGroup="+list);
		var listKey = {'keysgroup' : list, 
				'numberofmessages':this.reconciliationmsg.numberofmessages, 
				'purgeallrelatives': this.reconciliationmsg.purgeAllRelatives,
				'sendincomplete': this.reconciliationmsg.sendincomplete,
				'executecomplete': this.reconciliationmsg.executecomplete
		};
		this.inprogress=true;
		this.reconciliationmsg.showresult=false;
		this.sendPostAction(this, listKey, 'postSendReconciliation', 'sendincompletemessage')
		
	}
	/**
	 * receive status of execution
	 */
	this.postSendReconciliation = function( result ) {
		console.log("postSendReconciliation");
		this.reconciliationmsg.showresult=true;
		this.reconciliationmsg.execution = result; 
		
		// apply the statusExec on each message
		for (var i in this.reconciliationmsg.detection.listmessages) {
			var message = this.reconciliationmsg.detection.listmessages[ i ];
			// console.log("Check message");

			var oneexecution=false;
			var allexecution=true;
			for (var j in message.details) {
				var detail = message.details[ j ];
				// if this message was executed?
				// console.log("  Check Detail "+detail.wid);

				var foundexec=false;
				for (var k in this.reconciliationmsg.execution.details) {
					var detailexec = this.reconciliationmsg.execution.details[ k ];
					// console.log("     detail wid="+detail.wid+" <> "+detailexec.wid +" mid="+detail.mid+" <> "+detailexec.mid);
					if (detailexec.wid === detail.wid && detailexec.mid === detail.mid) {
						// console.log("  MATCH");
						detail.statusexec				= detailexec.statusexec;
						detail.explexec					= detailexec.explexec;
						detail.nbexecutioninprogress	= detailexec.nbexecutioninprogress;
						foundexec=true;
					}
				}

				if (foundexec) {
					oneexecution=true;
				} else {
					allexecution=false;
				}
				console.log("  Result Detail "+detail.wid+" FoundExec"+foundexec+" allexecution"+allexecution);

			} // end details
			
			if (oneexecution) {
				message.statusexec="(partial correction)";
			}
			if (allexecution) {
				message.statusexec="(complete correction)";
			}
			// console.log(" Result Message ["+message.statusexec+"]");

		}
	}
	
	// -----------------------------------------------------------------------------------------
	//  										PurgeTable
	// -----------------------------------------------------------------------------------------
	this.purgetablesmonitoring = {
			'monitoring' : {},
			'result' : {}
	}
	this.getPurgeTablesMonitoringItems = function() {
		console.log("start getPurgeTablesMonitoringItems");
		var self=this;
		self.inprogress=true;
		this.reconciliationmsg.showresult=false;

		// 7.6 : the server force a cache on all URL, so to bypass the cache, then create a different URL
		var d = new Date();
		
		$http.get( '?page=custompage_grumman&action=getpurgetablesmonitoring&t='+d.getTime() )
				.success( function ( jsonResult, statusHttp, headers, config ) {
					// connection is lost ?
					if (statusHttp==401 || typeof jsonResult === 'string') {
						console.log("Redirected to the login page !");
						window.location.reload();
					}
					console.log("getPurgeTablesMonitoringItems",jsonResult);
					self.purgetablesmonitoring.monitoring 		= jsonResult;
					self.inprogress=false;
				})
				.error( function() {
					self.inprogress=false;
					});
				
	}
	
	this.executepurgetablesmonitoring = function() {
		console.log("start purgetablesmonitoring");
		var self=this;
		self.inprogress=true;
		this.reconciliationmsg.showresult=false;

		// 7.6 : the server force a cache on all URL, so to bypass the cache, then create a different URL
		var d = new Date();
		
		$http.get( '?page=custompage_grumman&action=purgetablesmonitoring&t='+d.getTime() )
				.success( function ( jsonResult, statusHttp, headers, config ) {
					// connection is lost ?
					if (statusHttp==401 || typeof jsonResult === 'string') {
						console.log("Redirected to the login page !");
						window.location.reload();
					}
					console.log("getPurgeTablesMonitoringItems",jsonResult);
					self.purgetablesmonitoring.result 		= jsonResult;
					self.inprogress=false;
				})
				.error( function() {
					self.inprogress=false;
					});
				
	}
	
	// -----------------------------------------------------------------------------------------
	//  										SearchDuplicatelist
	// -----------------------------------------------------------------------------------------
	this.duplicationmsg = {
			'maximumnumberofduplications':500,
			'detection' : {},
			'execution' : {},
			'showdetailexecution' : 'hide'
	}
	this.getListDuplicateMessages = function() {
		console.log("start getListDuplicateMessages");
		var self=this;
		self.inprogress=true;
		

		// 7.6 : the server force a cache on all URL, so to bypass the cache, then create a different URL
		var d = new Date();
		
		$http.get( '?page=custompage_grumman&action=getlistduplicatemessages&maximumnumberofduplications='+this.duplicationmsg.maximumnumberofduplications+'&t='+d.getTime() )
				.success( function ( jsonResult, statusHttp, headers, config ) {
					// connection is lost ?
					if (statusHttp==401 || typeof jsonResult === 'string') {
						console.log("Redirected to the login page !");
						window.location.reload();
					}
					console.log("searchDuplication",jsonResult);
					self.duplicationmsg.detection 		= jsonResult;
					self.selectMessage( 'none', self.duplicationmsg.detection.listduplications);
					self.showDetailMessage( 'hide', self.duplicationmsg.detection.listduplications);
										
					self.inprogress=false;
				})
				.error( function() {
					self.inprogress=false;
					});
				
	}
	
	this.deleteDuplicateMessages = function() {
		console.log("start deleteduplicatemessages");
		var self=this;
		self.inprogress=true;

		// 7.6 : the server force a cache on all URL, so to bypass the cache, then create a different URL
		var d = new Date();
		var list= [];
		
		for (var i in this.duplicationmsg.detection.listduplications) {
			if (this.duplicationmsg.detection.listduplications[ i ].selected) {
				for (var j in this.duplicationmsg.detection.listduplications[ i ].messagesduplicated) {
					list.push( this.duplicationmsg.detection.listduplications[ i ].messagesduplicated[ j ].mid );
				}
			}
		}
		console.log("listIdToPurge="+list);
		var listKey = {'keys' : list};
		this.inprogress=true;
		this.sendPostAction(this, listKey, 'postSendDuplicateMessage', 'deleteduplicatemessages')		
	}
	
	
	this.postSendDuplicateMessage = function( result ) {
		// console.log("postSendDuplicateMessage -------------- ");
		this.duplicationmsg.showresult= true ;
		
		this.duplicationmsg.execution = result; 
		
		
		// apply the statusExec on each message
		for (var i in this.duplicationmsg.detection.listduplications) {
			var message = this.duplicationmsg.detection.listduplications[ i ];
			// console.log("Check message");

			var allexecution=true;
			for (var j in message.messagesduplicated) {
				var detail = message.messagesduplicated[ j ];
				// if this message was executed?
				// console.log("  Check Detail mid="+detail.mid);

				var foundexec=false;
				for (var k in this.duplicationmsg.execution.listmessageinstancepurged) {
					var detailexec = this.duplicationmsg.execution.listmessageinstancepurged[ k ];
					// console.log("     detail mid="+detail.mid+" <> "+detailexec);
					if (detailexec === detail.mid) {
						// console.log("  MATCH");
						detail.statusexec				= "purged";						
						foundexec=true;
					}
				}

				if (foundexec) {
				} else {
					allexecution=false;
				}
				// console.log("  Result Detail:"+detail.mid+" FoundExec:"+foundexec+" allexecution"+allexecution);

			} // end details
			
			if (allexecution) {
				message.statusexec="(purged)";
			}
		}
		
	}
	
	
	// ------------------------------------------------------------------------------------------------------
	// List URL (we miss the POST)
	// ------------------------------------------------------------------------------------------------------
	this.sendPost = {
		percent : 0,
		action : '',
		postAction : '',
		listUrls : [],
		listUrlsIndex : 0
	}
	this.sendPostAction = function( self, param, postAction, action ) // , listUrlCall,
													// listUrlIndex )
	{
		var self=this;
		
		// stop existing timer

		// the array maybe very big, so let's create a list of http call
		self.sendPost.percent=0;
		self.sendPost.listUrls=[];
		self.sendPost.listUrlsIndex=0;
		self.sendPost.action=action;
		self.sendPost.postAction = postAction;
		
		// prepare the string
		
		var json = angular.toJson( param, false);
		console.log("sendAllOnServer postAction["+postAction+"] action["+action+"] param["+json+"]");
		console.log("action["+action+"] Json="+json);

		
		// split the string by packet of 5000
		// first packet is a collect_reset, then a list of collect_add
		var loopaction="collect_reset";
		while (json.length>0)
		{
			var jsonFirst = encodeURIComponent( json.substring(0,5000));
			self.sendPost.listUrls.push( "action="+loopaction+"&paramjson="+jsonFirst);
			loopaction="collect_add";
			console.log(" listUrlPush :"+jsonFirst);
			json =json.substring(5000);
		}

		self.sendPost.listUrls.push( "action="+action);
		
		self.executeListUrl( self ) // , self.listUrlCall, self.listUrlIndex );
	};
	
	this.executeListUrl = function( self ) // , listUrlCall, listUrlIndex )
	{
		console.log(" Call "+self.sendPost.listUrlsIndex+" : "+self.sendPost.listUrls[ self.sendPost.listUrlsIndex ]);
		self.sendPost.percent= Math.round( (100 *  self.sendPost.listUrlsIndex) / self.sendPost.listUrls.length);
		
		$http.get( '?page=custompage_grumman&'+self.sendPost.listUrls[ self.sendPost.listUrlsIndex ]+'&t='+Date.now() )
			.success( function ( jsonResult, statusHttp, headers, config ) {
					
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();
				}
			
				// console.log("Correct, advance one more",
				// angular.toJson(jsonResult));
				self.sendPost.listUrlsIndex = self.sendPost.listUrlsIndex+1;
				if (self.sendPost.listUrlsIndex  < self.sendPost.listUrls.length )
					self.executeListUrl( self ) // , self.listUrlCall,
												// self.listUrlIndex);
				else
				{
					console.log("Finish", angular.toJson(jsonResult));
					self.inprogress=false;
					
					self.sendPost.percent= 100; 
					if (self.sendPost.postAction === 'postSendReconciliation') {
						self.postSendReconciliation( jsonResult );
					}
					if (self.sendPost.postAction === 'postSendDuplicateMessage') {
						self.postSendDuplicateMessage( jsonResult );
					}
					
					// post depending of the action
					
				}
			})
			.error( function() {
				self.wait=false;
				
				// alert('an error occure');
				});	
	};
	
	  
	// -----------------------------------------------------------------------------------------
	//  										Excel
	// -----------------------------------------------------------------------------------------

	this.exportReconciliationData = function () 
	{  
		//Start*To Export SearchTable data in excel  
	// create XLS template with your field.  
		var mystyle = {         
        headers:true,        
			columns: [  
			{ columnid: 'processname', title: 'Process Name'},
			{ columnid: 'processversion', title: 'Process Version'},
			{ columnid: 'flowname', title: 'Target Flow Name'},
			{ columnid: 'status', title: 'status'},
			{ columnid: 'caseid', title: 'Caseid'},
			{ columnid: 'messagename', title: 'Message Name'},
			{ columnid: 'wid', title: 'Waiting Id'},
			{ columnid: 'mid', title: 'Message Id'},
			{ columnid: 'expl', title: 'Explanation'},
			{ columnid: 'statusexec', title: 'Status Execution'}
			],         
		};  
	
        //get current system date.         
        var date = new Date();  
        $scope.CurrentDateTime = $filter('date')(new Date().getTime(), 'MM/dd/yyyy HH:mm:ss');          
		var trackingJson = [];
		for (var i in this.reconciliationmsg.detection.listmessages) {
			var message= this.reconciliationmsg.detection.listmessages[ i ];
			trackingJson.push( message );
			for (var j in message.details) {
				var detailsmerged = { ...message, ...message.details[ j ] };
				trackingJson.push( detailsmerged );
			}
		}
        //Create XLS format using alasql.js file.  
        alasql('SELECT * INTO XLS("Process_' + $scope.CurrentDateTime + '.xls",?) FROM ?', [mystyle, trackingJson]);  
    };
    this.exportDuplicateData = function() 
    {  
		//Start*To Export SearchTable data in excel  
	// create XLS template with your field.  
		var mystyle = {         
        headers:true,        
			columns: [  
			{ columnid: 'messagename', title: 'Message Name'},
			{ columnid: 'targetprocess', title: 'Target process'},
			{ columnid: 'targetflownode', title: 'Target FlowNode'},
			{ columnid: 'correlationvalues', title: 'Correlation values'},
			{ columnid: 'nbduplications', title: 'nbDuplications'},
			{ columnid: 'originalid', title: 'original Message ID'},
			{ columnid: 'messagesduplicatedst', title: 'Messages duplicated'},
			{ columnid: 'statusexec', title: 'Status Execution'}
			],         
		};  
	
        //get current system date.         
        var date = new Date();  
        $scope.CurrentDateTime = $filter('date')(new Date().getTime(), 'MM/dd/yyyy HH:mm:ss');          
		var trackingJson = [];
		for (var i in this.duplicationmsg.detection.listduplications) {
			var message= this.duplicationmsg.detection.listduplications[ i ];
			var detailsmerged = {...message };
			detailsmerged.messagesduplicatedst='';
			for (var j in message.messagesduplicated) {
				detailsmerged.messagesduplicated = detailsmerged.messagesduplicated + message.messagesduplicated[ j ].mid+",";
				console.log("Export, add "+message.messagesduplicated[ j ].mid+" => "+detailsmerged.messagesduplicated )
			}
			console.log("Detail="+angular.toJson( detailsmerged ));
			
			trackingJson.push( detailsmerged );
			
		}
        //Create XLS format using alasql.js file.  
        alasql('SELECT * INTO XLS("Process_' + $scope.CurrentDateTime + '.xls",?) FROM ?', [mystyle, trackingJson]);  
    };
	// -----------------------------------------------------------------------------------------
	//  										Properties
	// -----------------------------------------------------------------------------------------
	

	
	<!-- Manage the event -->
	this.getListEvents = function ( listevents ) {
		return $sce.trustAsHtml(  listevents );
	}
	

});



})();