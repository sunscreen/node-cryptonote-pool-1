var fs = require('fs');

var async = require('async');

var apiInterfaces = require('./apiInterfaces.js')(config.daemon, config.wallet);


var logSystem = 'payments';

var rpcRTX = {
    destinations: [],
    fee: config.payments.transferFee,
    mixin: config.payments.mixin,
    unlock_time: 0
    };

var rpcETX = {
    destinations: [],
    fee: config.payments.transferFee,
    mixin: config.payments.mixin,
    payment_id: '',
    unlock_time: 0
    };

require('./exceptionWriter.js')(logSystem);


log('info', logSystem, 'Started');


function isEmpty(value){
  return (value == null || value.length === 0);
}

function runInterval(){
    async.waterfall([

        //Get worker keys
        function(callback){
            redisClient.keys(config.coin + ':workers:*', function(error, result) {
                if (error) {
                    log('error', logSystem, 'Error trying to get worker balances from redis %j', [error]);
                    callback(true);
                    return;
                }
                callback(null, result);
            });
        },

        //Get worker balances
        function(keys, callback){
            var redisCommands = keys.map(function(k){
                return ['hget', k, 'balance'];
            });
            redisClient.multi(redisCommands).exec(function(error, replies){
                if (error){
                    log('error', logSystem, 'Error with getting balances from redis %j', [error]);
                    callback(true);
                    return;
                }
                var balances = [];
		var payees =[];
		var payids = {};
		var failures={};
                for (var i = 0; i < replies.length; i++){
                    var parts = keys[i].split(':');
		    //console.log(parts);
                    var workerId = parts[2];
		    var payID = parts[3];
		
		    if ((workerId in balances)) { 
		    log('info',logSystem,'Payment id %s found for wallet address %s balance: %d',[payID,workerId,balances[workerId]]);
		    payees[workerId]=workerId.split(".");
		    payids[workerId]=payID;
		    }  else {
                    balances[workerId]= parseInt(replies[i]) || 0
		    }

		    if (payID == undefined) { 
		    payids[workerId] = false; 
		    } else {
		    payids[workerId] = payID;
		    }
		    
                }
                callback(null, balances, payids,payees);
            });
        },

        //Filter workers under balance threshold for payment
        function(balances, payids,payees, callback){
            var payments = {};

            for (var worker in balances){
                var balance = balances[worker];
                if (balance >= config.payments.minPayment){
                    var remainder = balance % config.payments.denomination;
                    var payout = balance - remainder;
                    if (payout < 0) continue;
                    payments[worker] = payout;
                }
            }

            if (Object.keys(payments).length === 0){
                log('info', logSystem, 'No workers\' balances reached the minimum payment threshold');
                callback(true);
                return;
            }

            var transferCommands = [];

/*            var transferCommandsLength = Math.ceil(Object.keys(payments).length / config.payments.maxAddresses);

            for (var i = 0; i < transferCommandsLength; i++){

                transferCommands.push({
                    redis: [],
                    amount : 0,
                    rpc: {
                        destinations: [],
                        fee: config.payments.transferFee,
                        mixin: config.payments.mixin,
                        unlock_time: 0,
			payment_id:''
                    }
                });


            }
*/

            var addresses = 0;
            var commandIndex = 0;
            for (var worker in payments){
                var amount = parseInt(payments[worker]);
		var Payid = payids[worker];

		if (Payid == false) {
                transferCommands.push({redis:[],amount : 0,rpc: rpcRTX});
                transferCommands[commandIndex].rpc.destinations.push({amount: amount, address: worker});
                log('info', logSystem, 'Regular TX Detected using rpcRTX');
		} else {

		var cleanworker = payees[worker];
		var clean = cleanworker[Object.keys(cleanworker)[Object.keys(cleanworker).length - 2]].toString();
		
                transferCommands.push({redis:[],amount : 0,rpc: rpcETX});
                log('info', logSystem, 'Exchange TX Detected using rpcETX worker: %s paymentid: %s',[clean,Payid]);
		transferCommands[commandIndex].rpc.destinations=[]; /* Reset destination for rpcETX Failure */
//		transferCommands[commandIndex].rpc.destinations.push({amount: 00000000000,address:'47sghzufGhJJDQEbScMCwVBimTuq6L5JiRixD8VeGbpjCTA12noXmi4ZyBZLc99e66NtnKff34fHsGRoyZk3ES1s1V4QVcB'}); 
                transferCommands[commandIndex].rpc.destinations.push({amount: amount, address: clean});
		var p_id= payids[worker];
                transferCommands[commandIndex].rpc.payment_id = p_id;
/*                transferCommands[commandIndex].rpc.payment_id = '055bcd0206e0f4e8b52cc7b498797b3fd8530c564ae75a115657505dc7e7b8fe'; */

		}

                transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'balance', -amount]);
                transferCommands[commandIndex].redis.push(['hincrby', config.coin + ':workers:' + worker, 'paid', amount]);
                transferCommands[commandIndex].amount += amount;

                addresses++;
                if (addresses >= config.payments.maxAddresses || Payid != false ){
                    commandIndex++;
                    addresses = 0;
                }

            }

            var timeOffset = 0;
	    var etxerrorcount = 0;
	    var etxcoumt=0;
            async.filter(transferCommands, function(transferCmd, cback){
                apiInterfaces.rpcWallet('transfer', transferCmd.rpc, function(error, result){
		    //console.log(error);
                    if (error){
                        log('error', logSystem, 'Error with transfer RPC request to wallet daemon %j', [error]);
                        log('error', logSystem, 'Payments failed to send to %j', transferCmd.rpc.destinations);
                        cback(false);
                        return;
                    }
                    var now = (timeOffset++) + Date.now() / 1000 | 0;
                    var txHash = result.tx_hash.replace('<', '').replace('>', '');

                    transferCmd.redis.push(['zadd', config.coin + ':payments:all', now, [
                        txHash,
                        transferCmd.amount,
                        transferCmd.rpc.fee,
                        transferCmd.rpc.mixin,
                        Object.keys(transferCmd.rpc.destinations).length
                    ].join(':')]);


                    for (var i = 0; i < transferCmd.rpc.destinations.length; i++){
                        var destination = transferCmd.rpc.destinations[i];
                        transferCmd.redis.push(['zadd', config.coin + ':payments:' + destination.address, now, [
                            txHash,
                            destination.amount,
                            transferCmd.rpc.fee,
                            transferCmd.rpc.mixin
                        ].join(':')]);
                    }

                    log('info', logSystem, 'Payments sent via wallet daemon %j', [result]);
                    redisClient.multi(transferCmd.redis).exec(function(error, replies){

                        if (error){
                            log('error', logSystem, 'Super critical error! Payments sent yet failing to update balance in redis, double payouts likely to happen %j', [error]);
                            log('error', logSystem, 'Double payments likely to be sent to %j', transferCmd.rpc.destinations);
                            cback(false);
                            return;
                        }
                        cback(true);
                    });
                });
            }, function(succeeded){
                var failedAmount = transferCommands.length - succeeded.length;

                log('info', logSystem, 'Payments splintered and %d successfully sent, %d failed', [succeeded.length, failedAmount]);
                callback(null);
            });

        }

    ], function(error, result){
        setTimeout(runInterval, config.payments.interval * 1000);
    });
}

runInterval();