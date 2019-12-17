package Exchange::Main;
use Moose::Role;
use Method::Signatures::Simple;
#use Method::Manual::MethodModifiers;

####////}}}}

use JSON;
use Data::Dumper;
use AnyEvent;
#use Net::RabbitFoot;
# use Coro;
use Net::RabbitMQ;
use TryCatch;

#### Strings
has 'apiroot'	=>	( isa => 'Str', is => 'rw', default => " http://localhost:55672/api/vhosts");
has 'sendtype'	=> 	( isa => 'Str|Undef', is => 'rw', default => "response" );
has 'sourceid'	=>	( isa => 'Undef|Str', is => 'rw', default => "" );
has 'callback'	=>	( isa => 'Undef|Str', is => 'rw', default => "" );
has 'queue'		=>	( isa => 'Undef|Str', is => 'rw', default => undef );
has 'token'		=> ( isa => 'Str|Undef', is => 'rw' );
has 'user'		=> ( isa => 'Str|Undef', is => 'rw', required	=>	0 );
has 'pass'		=> ( isa => 'Str|Undef', is => 'rw', required	=>	0 );
has 'host'		=> ( isa => 'Str|Undef', is => 'rw', required	=>	0 );
has 'vhost'		=> ( isa => 'Str|Undef', is => 'rw', required	=>	0 );
has 'port'		=> ( isa => 'Str|Undef', is => 'rw', default	=>	5672 );
has 'sleep'		=>  ( isa => 'Int', is => 'rw', default => 2 );

#### Objects
#has 'connection'=> ( isa => 'Net::RabbitFoot', is => 'rw', lazy	=> 1, builder => "openConnection" );
has 'connection'=> ( isa => 'Net::RabbitMQ', is => 'rw', lazy	=> 1, builder => "getConnection" );
has 'storedconnection'=> ( isa => 'Net::RabbitMQ', is => 'rw' );
has 'receiveconnection'=> ( isa => 'Net::RabbitMQ', is => 'rw' );
#has 'channel'	=> ( isa => 'Net::RabbitFoot::Channel', is => 'rw', lazy	=> 1, builder => "openConnection" );

#### NOTIFY
method notifyStatus ($data, $status) {
	$self->logDebug("status", $status);
	#$self->logDebug("data", $data);

	$data->{status}	=	$status;
	$self->notify($data);
}

method notifyError ($data, $error) {
	$self->logDebug("error", $error);
	$data->{error}	=	$error;
	
	$self->notify($data);
}

method notify ($data) {
	my $hash		=	{};
	$hash->{data}	=	$data;
	$hash			=	$self->addIdentifiers($hash);
	#$self->logDebug("hash", $hash);
	
	my $parser		=	$self->setParser();
	my $message		=	$parser->encode($hash);

	$self->sendFanout('chat', $message);
}

#### FANOUT
method sendFanout ($exchange, $message) {
	#$self->logDebug("exchange", $exchange);
	#$self->logDebug("message", $message);
	
	my $host	=	$self->host() || $self->conf()->getKey("queue:host", undef);my $port	=	$self->port() || $self->conf()->getKey("queue:port", undef);
	my $user	= 	$self->user() || $self->conf()->getKey("queue:user", undef);
	my $pass	=	$self->pass() || $self->conf()->getKey("queue:pass", undef);
	my $vhost	=	$self->vhost() || $self->conf()->getKey("queue:vhost", undef);

	my $connection 	=	$self->getConnection();
	#my $connection      = Net::RabbitMQ->new();
	$self->logDebug("connection", $connection);

	#my $chanID  = nextchan();
	my $chanID = 1;
	
	#my %qparms = (
	#	user => "guest",
	#	password => "guest",
	#	host    =>  $host,
	#	vhost   =>  "/",
	#	port    =>  5672
	#);

	try {
		#$self->logDebug("BEFORE connection->connect()");
		#$connection->connect($host, \%qparms);
		#$self->logDebug("AFTER connection->connect()");
		
		#$connection->channel_open($chanID);
		
		#my %declare_opts = (
		#	durable => 1,
		#	auto_delete => 0
		#);
		#
		#$self->logDebug("DOING connection->queue_declare($chanID, $exchange, declare_opts", \%declare_opts);
		#$connection->queue_declare($chanID, $exchange, \%declare_opts,);
		
		$connection->publish($chanID, $exchange, $message,{
			exchange 	=> "chat",
			routing_key =>  "",
			durable 	=> 1
		},);
		
		print " [x] Sent fanout on host $host, exchange $exchange: ", substr($message, 0, 400), "\n";
		
		#$connection->disconnect();
	
	}
	catch {
		$self->logDebug("Failed to connect");
	}
}

method receiveFanout ($exchange, $handler) {
	$self->logDebug("exchange", $exchange);
	$self->logDebug("handler", $handler);
    $|++;
    
    #my $exchange	=	"chat";
	my $channelid	=	1;
	#my $host		=	$self->host() || $self->conf()->getKey("queue:host", undef);
	#$self->logDebug("host", $host);

	#### 1. CONNECTION
	my $connection      = $self->newConnection();
	$self->receiveconnection($connection);
	#my $connection      = Net::RabbitMQ->new() ;
	#my $channelid  = 1;
	#my %qparms = (
	#	user 		=> "guest",
	#	password 	=> "guest",
	#	host    	=>  $host,
	#	vhost   	=>  "/",
	#	port   		=>  5672
	#);
	#$self->logDebug("DOING connection->connect()");
	#$connection->connect($host, \%qparms);
	#
	#### 2. CHANNEL
	$self->logDebug("DOING connection->channel_open()");
	$connection->channel_open($channelid);
	
	$self->logDebug("DOING connection->basic_qos()");
	#### TELL RabbitMQ TO ONLY GIVE ONE MESSAGE AT A TIME TO WORKER
	$connection->basic_qos($channelid,{ prefetch_count => 1 });
	
	#### 3. EXCHANGE
	# NB: DEFAULTS
	# exchange_type 	=> $type,  		# default 'direct'
	# passive 			=> $boolean,    # default 0
	# durable 			=> $boolean,    # default 0
	# auto_delete 		=> $boolean,  	# default 1
	my $options = {
		exchange_type=>	"fanout",
		autodelete	=> 	0,
		auto_delete	=> 	0,
		durable 	=> 	0
	};
	$self->logDebug("DOING exchange_declare($channelid, $exchange, options)");
	$connection->exchange_declare($channelid, $exchange, $options);
	
	#### 4. QUEUE
	my %queue_opts = (
		durable => 1,
		auto_delete => 0
	);
	$self->logDebug("DOING connection->queue_declare()");
	$connection->queue_declare($channelid, $exchange, \%queue_opts) ;
	$self->logDebug("DOING connection->queue_bind()");
	$connection->queue_bind($channelid, $exchange, "chat", "",);

	#### 5. CONSUME
	my %consume_opts = (
		exchange => "chat",
		routing_key =>  ""
	);
	$connection->consume($channelid, $exchange, \%consume_opts);
	$self->logDebug("connection", $connection);
	
	# NOTE: recv() is BLOCKING
	while ( my $payload = $connection->recv() ) {

		$self->logDebug("payload", $payload);
		#$self->logDebug("payload", substr($payload, 0, 400));
		next if not defined $payload;
		my $body  = $payload->{body};
		#$self->logDebug("received body", substr($body, 0, 100));
		my $dtag  = $payload->{delivery_tag} ;
	
		print "[x] Received from queue $exchange: ", substr($body, 0, 400), "\n";
		$self->$handler($body);
		#$self->handleFanout($body);
		
		#$connection->ack($channelid,$dtag,);
	}
}

method exchangeExists ($exchangename) {
	my $apiroot = $self->apiroot();
}

#### SEND SOCKET
method sendSocket ($data) {	
	$self->logDebug("");
	$self->logDebug("data", $data);

	#### BUILD RESPONSE
	$data->{username}	=	$self->username();
	$data->{sourceid}	=	$self->sourceid();
	$data->{callback}	=	$self->callback();
	$data->{token}		=	$self->token();
	$data->{sendtype}	=	"data";

	$self->sendData($data);
}

method sendData ($data) {
	#### CONNECTION
	my $connection		=	$self->newSocketConnection($data);
	my $channel 		=	$connection->open_channel();
	my $exchange		=	$self->conf()->getKey("queue:exchange", undef);
	my $exchangetype	=	$self->conf()->getKey("queue:exchangetype", undef);
	#$self->logDebug("$$    exchange", $exchange);
	#$self->logDebug("$$    exchangetype", $exchangetype);

	$channel->declare_exchange(
		exchange 	=> 	$exchange,
		type 		=> 	$exchangetype,
	);

	my $jsonparser 	= 	JSON->new();
	my $json 		=	$jsonparser->encode($data);
	$self->logDebug("$$    json", substr($json, 0, 1500));
	
	$channel->publish(
		exchange => $exchange,
		routing_key => '',
		body => $json,
	);

	my $host = $data->{host} || $self->conf()->getKey("queue:host", undef);
	print "[*]   $$   [$host|$exchange|$exchangetype] Sent message: ", substr($json, 0, 1500), "\n";
	
	$connection->close();
}

method receiveSocket ($data) {

	$data 	=	{}	if not defined $data;
	$self->logDebug("data", $data);

	my $connection	=	$self->newSocketConnection($data);
	$self->logDebug("connection", $connection);
	
	my $channel = $connection->open_channel();
	
	#### GET EXCHANGE INFO
	my $exchange		=	$self->conf()->getKey("queue:exchange", undef);
	my $exchangetype	=	$self->conf()->getKey("queue:exchangetype", undef);
	$self->logDebug("exchange", $exchange);
	$self->logDebug("exchangetype", $exchangetype);

	$channel->declare_exchange(
		exchange 	=> 	$exchange,
		type 		=> 	$exchangetype,
	);
	$self->logDebug("AFTER");
	
	my $result = $channel->declare_queue( exclusive => 1, );	
	my $queuename = $result->{method_frame}->{queue};
	
	$channel->bind_queue(
		exchange 	=> 	$exchange,
		queue 		=> 	$queuename,
	);

	#### REPORT	
	my $host = $data->{host} || $self->conf()->getKey("socket:host", undef);
	$self->logDebug(" [*] [$host|$exchange|$exchangetype|$queuename] Waiting for RabbitJs socket traffic");
	print " [*] [$host|$exchange|$exchangetype|$queuename] Waiting for RabbitJs socket traffic\n";
	
	sub callsub {
		my $var = shift;
		my $body = $var->{body}->{payload};
	
		print " [x] Received message: ", substr($body, 0, 500), "\n";
	}
	
	$channel->consume(
		on_consume 	=> 	\&callsub,
		queue 		=> 	$queuename,
		no_ack 		=> 	1
	);
	
	AnyEvent->condvar->recv;
}

method addIdentifiers ($data) {
	#$self->logDebug("data", $data);
	#$self->logDebug("self: $self");
	
	#### SET TOKEN
	$data->{token}		=	$self->token();
	
	#### SET SENDTYPE
	$data->{sendtype}	=	$self->sendtype();
	
	#### SET DATABASE
	$self->setDbh() if not defined $self->db();
	$data->{database} 	= 	$self->db()->database() || "";

	#### SET USERNAME		
	$data->{username} 	= 	$self->username();

	#### SET SOURCE ID
	$data->{sourceid} 	= 	$self->sourceid();
	
	#### SET CALLBACK
	$data->{callback} 	= 	$self->callback();
	
	$self->logDebug("Returning data", $data);

	return $data;
}


##### CONNECTION
method getConnection {
	$self->logDebug("self->storedconnection()", $self->storedconnection());

	return $self->storedconnection() if defined $self->storedconnection();

	my $connection = $self->newConnection();	
	my $channelid = 1;
	my $channel = $connection->channel_open($channelid);
	#$self->logDebug("BEFORE channel", $channel);

	#### GET EXCHANGE INFO
	my $exchange		=	$self->conf()->getKey("queue:exchange", undef);
	my $exchangetype	=	$self->conf()->getKey("queue:exchangetype", undef);

	$exchangetype = "fanout";
	$self->logDebug("exchange", $exchange);
	$self->logDebug("exchangetype", $exchangetype);

	#### SET DEFAULT CHANNEL
	#$self->setChannel($exchange, $exchangetype);	
	#$self->channel()->declare_exchange(
	#	exchange => $name,
	#	type => $type,
	#);
	#
	#$channel->declare_exchange(
	#	exchange => 'chat',
	#	type => 'fanout',
	#);

	try {
		$connection->exchange_declare(
			$channelid,
			'chat',
			{
				exchange_type 	=> 	'fanout',
				passive			=> 	0,
				#durable 		=>	1,
				durable 		=>	0,
				auto_delete 	=> 	0
			}
		);
	}
	catch {
		$self->logDebug("Failed to declare exchange $exchange. It's probably already declared");
	}
	
	#### DECLARE QUEUE
	my %declare_opts = (
		durable => 1,
		auto_delete => 0
	);
	
	$self->logDebug("DOING connection->queue_declare($channelid, $exchange, declare_opts", \%declare_opts);
	$connection->queue_declare($channelid, $exchange, \%declare_opts,);

	$self->storedconnection($connection);

	return $connection;	
}

method newConnection {
	my $host		=	$self->conf()->getKey("queue:host", undef);
	my $user		= 	$self->conf()->getKey("queue:user", undef);
	my $password		=	$self->conf()->getKey("queue:pass", undef);
	my $vhost		=	$self->conf()->getKey("queue:vhost", undef);
	
	$self->logDebug("host", $host);
	$self->logDebug("user", $user);
	$self->logDebug("password", $password);
	$self->logDebug("vhost", $vhost);
	
	my $connection  = Net::RabbitMQ->new();
	$connection->connect(
		$host,
		{
			port 		=>	5672,
			host		=>	$host,
			user 		=>	$user,
			password 	=>	$password,
			vhost		=>	$vhost
		}
	);
	$self->logDebug("connection", $connection);

	return $connection;	
}

#### TASK
method sendTask ($queuename, $data) {
	$self->logDebug("queuename", $queuename);
	$self->logDebug("data", $data);
	$self->logDebug("self->storedconnection()", $self->storedconnection());

	return $self->storedconnection() if defined $self->storedconnection();

	my $connection = $self->newConnection();	
	my $channelid = 1;
	my $channel = $connection->channel_open($channelid);
	#$self->logDebug("BEFORE channel", $channel);

	$connection->queue_declare(
		$channelid,
		$queuename,
		{
			durable => 1,
			auto_delete => 0
		}
	);
	
	my $jsonparser	=	JSON->new();
	$self->logDebug("data hash: $data");
	$self->logDebug("data", $data);
	# my $message		=	$jsonparser->encode->allow_nonref(my $nonmagical = $data);
	my $message		=	encode_json($data);
	$self->logDebug("message", $message);

	$connection->publish(
		$channelid,
		$queuename,
		$message,
		{
			exchange => ""
		}
		,
	);

    print "Message sent to queue $queuename: $message\n";
}

method receiveTask ($queuename, $handler) {
	
	$self->logDebug("queuename", $queuename);
	$self->logDebug("handler", $handler);

	#### NB: (RECEIVER) QUEUENAME = ROUTING_KEY (SENDER)

	#### CONNECTION
	my $connection = $self->newConnection();	
	my $channelid = 1;
	my $channel = $connection->channel_open($channelid);

	#### QUEUE DECLARE
	$connection->queue_declare(
		$channelid,
		$queuename,
		{
			durable => 1,
			auto_delete => 0
		}
	);

	#### CONSUME
	$connection->consume(
		$channelid,
		$queuename,
		{
			#exchange => "chat",
			#routing_key =>  ""
		}
	) ;

	#### NOTE THAT recv() is BLOCKING!!!
	while ( my $payload = $connection->recv() )
	{
		last if not defined $payload ;
		my $body  = $payload->{body} ;
		my $dtag  = $payload->{delivery_tag} ;
		my ($sec) = ( $body =~ m{(\d+)} ) ;

		print "[x] Received from queue $queuename: ", substr($body, 0, 400), "\n";

		$self->$handler($body);

		$connection->ack($channelid,$dtag,) ;
	}
}

#### UTILS
method startRabbitJs {
	my $command		=	"service rabbitjs restart";
	$self->logDebug("command", $command);
	
	return `$command`;
}

method stopRabbitJs {
	my $command		=	"service rabbitjs stop";
	$self->logDebug("command", $command);
	
	return `$command`;
}


method setChannel($name, $type) {
	$self->channel()->declare_exchange(
		exchange => $name,
		type => $type,
	);
}

method closeConnection {
	$self->logDebug("self->connection()", $self->connection());
	$self->connection()->close();
}





method setParser {
	return JSON->new->allow_nonref;
}

1;



#method receiveFanout ($exchange) {
#	$self->logDebug("exchange", $exchange);
#	
#	my $host		=	$self->host() || $self->conf()->getKey("queue:host", undef);
#	my $user		= 	$self->user() || $self->conf()->getKey("queue:user", undef);
#	my $pass		=	$self->pass() || $self->conf()->getKey("queue:pass", undef);
#	my $vhost		=	$self->vhost() || $self->conf()->getKey("queue:vhost", undef);
#
#	my $qname   = q{chat} ;
#	
#	### NO CONFIGURABLE PARAMETERS BELOW THIS LINE #########################
#	my $connection = $self->connection();
#	$self->logDebug("connection", $connection);
#
#	#my $connection      = Net::RabbitMQ->new() ;
#	my $chanID  = 1;
#	
#	#my %qparms = (
#	#	user => "guest",
#	#	password => "guest",
#	#	host    =>  $host,
#	#	vhost   =>  "/",
#	#	port    =>  5672
#	#);
#	#$connection->connect($host, \%qparms);
#	
#	$connection->channel_open($chanID);
#	
#	$connection->basic_qos($chanID,{ prefetch_count => 1 });
#	
#	my %declare_opts = (
#		durable => 1,
#		auto_delete => 0
#	);
#	
#	$self->logDebug("DOING connection->queue_declare($chanID, $exchange, declare_opts", \%declare_opts);
#	$connection->queue_declare($chanID, $qname, \%declare_opts);
#	
#	#### IMPORTANT: REQUIRED TO CONNECT TO chat FANOUT
#	$connection->queue_bind($chanID, $qname, "chat", "",);
#	
#	my %consume_opts = (
#		exchange => "chat",
#		routing_key =>  ""
#	);
#	$connection->consume($chanID, $qname, \%consume_opts);
#	
#	#### NB: BLOCKING
#	while ( my $payload = $connection->recv() ) {
#		last if not defined $payload;
#		my $body  = $payload->{body};
#		my $dtag  = $payload->{delivery_tag};
#		
#		print qq{[x] Received from queue $qname: $body\n};
#	
#		$connection->ack($chanID,$dtag,);
#	}
#}


##### DELETE LATER
#method newSocketConnection ($args) {
#	$self->logCaller("");
#	$self->logDebug("args", $args);
#
#	my $host = $args->{host} || $self->conf()->getKey("queue:host", undef);
#	my $port = $args->{port} || $self->conf()->getKey("queue:port", undef);
#	my $user = $args->{user} || $self->conf()->getKey("queue:user", undef);
#	my $password = $args->{password} || $self->conf()->getKey("queue:password", undef);
#	my $vhost = $args->{vhost} || $self->conf()->getKey("queue:vhost", undef);
#	$self->logDebug("host", $host);
#	$self->logDebug("port", $port);
#	$self->logDebug("user", $user);
#	$self->logDebug("password", $password);
#	$self->logDebug("host", $host);
#
#	my $connection = Net::RabbitFoot->new()->load_xml_spec()->connect(
#		host => $host,
#		port => $port,
#		user => $user,	
#		password => $password,
#		vhost => $vhost,
#	);
#	#$self->logDebug("connection", $connection);
#	
#	return $connection;	
#}


##around logError => sub {
##	my $orig	=	shift;
##    my $self 	= 	shift;
##	my $error	=	shift;
##	
##	$self->logCaller("XXXXXXXXXXXXXXXX");
##	$self->logDebug("self->logtype", $self->logtype());
##
##	#### DO logError
##	$self->$orig($error);
##	
##	warn "Error: $error" and return if $self->logtype() eq "cli";
##
##	#### SEND EXCHANGE MESSAGE
##	my $data	=	{
##		error	=>	$error,
##		status	=>	"error"
##	};
##	$self->sendSocket($data);
##};
##


#### TASK
#method receiveTask ($taskqueue) {
#	$self->logDebug("taskqueue", $taskqueue);
#	
#	#### OPEN CONNECTION
#	my $connection	=	$self->newConnection();	
#	my $channel 	= 	$connection->open_channel();
#	#$self->channel($channel);
#	$channel->declare_queue(
#		queue => $taskqueue,
#		durable => 1,
#	);
#	
#	print "[*] Waiting for tasks in queue: $taskqueue\n";
#	
#	$channel->qos(prefetch_count => 1,);
#	
#	no warnings;
#	my $handler	= *handleTask;
#	use warnings;
#	my $this	=	$self;
#
#	#### GET HOST
#	my $host		=	$self->conf()->getKey("queue:host", undef);
#	
#	print " [x] Receiving tasks in host $host taskqueue '$taskqueue'\n";
#	
#	$channel->consume(
#		on_consume	=>	sub {
#			my $var 	= 	shift;
#			#print "Exchange::receiveTask    DOING CALLBACK";
#		
#			my $body 	= 	$var->{body}->{payload};
#			print " [x] Received task in host $host taskqueue '$taskqueue': $body\n";
#		
#			my @c = $body =~ /\./g;
#		
#			#### RUN TASK
#			&$handler($this, $body);
#			
#			my $sleep	=	$self->sleep();
#			#print "Sleeping $sleep seconds\n";
#			sleep($sleep);
#			
#			#### SEND ACK AFTER TASK COMPLETED
#			print "Exchange::receiveTask    sending ack\n";
#			$channel->ack();
#		},
#		no_ack => 0,
#	);
#	
#	#### SET self->connection
#	$self->connection($connection);
#	
#	# Wait forever
#	AnyEvent->condvar->recv;	
#}
#
#method handleTask ($json) {
#	$self->logDebug("json", substr($json, 0, 1000));
#
#	my $data = $self->jsonparser()->decode($json);
#	#$self->logDebug("data", $data);
#	
#	#####my $duplicate	=	$self->duplicate();
#	#####if ( defined $duplicate and not $self->deeplyIdentical($data, $duplicate) ) {
#	#####	$self->logDebug("Skipping duplicate message");
#	#####	return;
#	#####}
#	#####else {
#	#####	$self->duplicate($data);
#	#####}
#	
#	my $mode =	$data->{mode} || "";
#	#$self->logDebug("mode", $mode);
#	
#	if ( $self->can($mode) ) {
#		$self->$mode($data);
#	}
#	else {
#		print "mode not supported: $mode\n";
#		$self->logDebug("mode not supported: $mode");
#	}
#}
#
#method sendTask ($task) {
#	$self->logDebug("task", $task);
#	my $processid	=	$$;
#	$self->logDebug("processid", $processid);
#	$task->{processid}	=	$processid;
#
#	#### SET QUEUE
#	$task->{queue} = $self->setQueueName($task) if not defined $task->{queue};
#	my $queuename		=	$task->{queue};
#	$self->logDebug("queuename", $queuename);
#	
#	#### ADD UNIQUE IDENTIFIERS
#	$task	=	$self->addTaskIdentifiers($task);
#
#	my $jsonparser = JSON->new();
#	my $json = $jsonparser->encode($task);
#	$self->logDebug("json", $json);
#
#	#### GET CONNECTION
#	my $connection	=	$self->newConnection();
#	$self->logDebug("DOING connection->open_channel()");
#	my $channel = $connection->open_channel();
#	$self->channel($channel);
#	#$self->logDebug("channel", $channel);
#	
#	$channel->declare_queue(
#		queue => $queuename,
#		durable => 1,
#	);
#	
#	#### BIND QUEUE TO EXCHANGE
#	$channel->publish(
#		exchange 		=> '',
#		routing_key 	=> $queuename,
#		body 			=> $json
#	);
#
#	my $host		=	$self->conf()->getKey("queue:host", undef);
#	
#	print " [x] Sent task in host $host taskqueue '$queuename': '$json'\n";
#
#	$self->logDebug("closing connection");
#	$connection->close();
#}
#
#method addTaskIdentifiers ($task) {
#	
#	#### SET TOKEN
#	$task->{token}		=	$self->token();
#	
#	#### SET SENDTYPE
#	$task->{sendtype}	=	"task";
#	
#	#### SET DATABASE
#	$self->setDbh() if not defined $self->db();
#	$task->{database} 	= 	$self->db()->database() || "";
#
#	#### SET SOURCE ID
#	$task->{sourceid} 	= 	$self->sourceid();
#	
#	#### SET CALLBACK
#	$task->{callback} 	= 	$self->callback();
#	
#	$self->logDebug("Returning task", $task);
#	
#	return $task;
#}
#method setQueueName ($task) {
#	$self->logDebug("task", $task);
#	
#	#### VERIFY VALUES
#	my $notdefined	=	$self->notDefined($task, ["username", "project", "workflow"]);
#	$self->logDebug("notdefined", $notdefined);
#	
#	$self->logCritical("notdefined: @$notdefined") and return if @$notdefined;
#	
#	my $username	=	$task->{username};
#	my $project		=	$task->{project};
#	my $workflow	=	$task->{workflow};
#	my $queue		=	"$username.$project.$workflow";
#	#$self->logDebug("queue", $queue);
#	
#	return $queue;	
#}
#
#method notDefined ($hash, $fields) {
#	return [] if not defined $hash or not defined $fields or not @$fields;
#	
#	my $notDefined = [];
#    for ( my $i = 0; $i < @$fields; $i++ ) {
#        push( @$notDefined, $$fields[$i]) if not defined $$hash{$$fields[$i]};
#    }
#
#    return $notDefined;
#}
#
#
#
###### TOPIC
#method listenTopics {
#	$self->logDebug("");
#	my $childpid = fork;
#	if ( $childpid ) #### ****** Parent ****** 
#	{
#		$self->logDebug("PARENT childpid", $childpid);
#	}
#	elsif ( defined $childpid ) {
#		$self->receiveTopic();
#	}
#}
#
#method receiveTopic ($message) {
#	$self->logDebug("message", $message);
#
#	#### OPEN CONNECTION
#	my $connection	=	$self->newConnection();	
#	my $channel = $connection->open_channel();
#
#	my $exchange	=	$self->exchange();
#	$self->logDebug("exchange", $exchange);
#	
#	$channel->declare_exchange(
#		exchange => $exchange,
#		type => 'topic',
#	);
#	
#	my $result = $channel->declare_queue(exclusive => 1);
#	my $queuename = $result->{method_frame}->{queue};
#	
#	my $keys	=	$self->keys();
#	$self->logDebug("keys", $keys);
#
#	for my $key ( @$keys ) {
#		$channel->bind_queue(
#			exchange => $exchange,
#			queue => $queuename,
#			routing_key => $key,
#		);
#	}
#	
#	print " [*] Listening for topics: @$keys\n";
#
#	no warnings;
#	my $handler	= *handleTopic;
#	use warnings;
#	my $this	=	$self;
#
#	$channel->consume(
#        on_consume => sub {
#			my $var = shift;
#			my $body = $var->{body}->{payload};
#		
#			print " [x] Received message: $body\n";
#			&$handler($this, $body);
#		},
#		no_ack => 1,
#	);
#	
#	# Wait forever
#	AnyEvent->condvar->recv;	
#}
#
#method handleTopic ($json) {
#	$self->logDebug("json", $json);
#
#	my $data = $self->jsonparser()->decode($json);
#	#$self->logDebug("data", $data);
#
#	my $mode =	$data->{mode};
#	$self->logDebug("mode", $mode);
#	
#	if ( $mode eq "hostStatus" ) {
#		$self->updateHostStatus($data);
#	}
#	else {
#		$self->updateTaskStatus($data);
#	}
#}
#
#
