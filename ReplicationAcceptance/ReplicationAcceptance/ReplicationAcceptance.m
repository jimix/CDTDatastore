//
//  ReplicationAcceptance.m
//  ReplicationAcceptance
//
//  Created by Michael Rhodes on 29/01/2014.
//
//

#import "ReplicationAcceptance.h"

#import <XCTest/XCTest.h>

#import <CloudantSync.h>
#import <UNIRest.h>
#import <TRVSMonitor.h>

#import "CloudantReplicationBase.h"
#import "CloudantReplicationBase+CompareDb.h"
#import "ReplicationAcceptance+CRUD.h"
#import "ReplicatorDelegates.h"
#import "ReplicatorURLProtocol.h"
#import "ReplicatorURLProtocolTester.h"

#import "CDTDatastoreManager.h"
#import "CDTDatastore.h"
#import "CDTDocumentBody.h"
#import "CDTDocumentRevision.h"
#import "CDTPullReplication.h"
#import "CDTPushReplication.h"
#import "TDReplicatorManager.h"
#import "TDReplicator.h"
#import "CDTReplicator.h"
#import "CDTLogging.h"
@interface ReplicationAcceptance ()

/** This database is used as the primary remote database. Some tests create further
 databases, but all use this one.
 */
@property (nonatomic, strong) NSString *primaryRemoteDatabaseName;

@end

@implementation ReplicationAcceptance

/**
 This is the standard number of documents those tests requiring a number
 of documents to replicate use. 10k takes 50 minutes, 100k much longer,
 as all these documents are read from both local and remote databases
 during the check phase.
 */
static NSUInteger n_docs = 10000;
/**
 Rev tree size for "large rev tree" tests.
 */
static NSUInteger largeRevTreeSize = 1500;

#pragma mark - setUp and tearDown

- (void)setUp
{
    [super setUp];

    // Create local and remote databases, start the replicator

    NSError *error;
    self.datastore = [self.factory datastoreNamed:@"test" error:&error];
    XCTAssertNotNil(self.datastore, @"datastore is nil");

    self.primaryRemoteDatabaseName = [NSString stringWithFormat:@"%@-test-database-%@",
                                    self.remoteDbPrefix,
                                    [CloudantReplicationBase generateRandomString:5]];
    self.primaryRemoteDatabaseURL = [self.remoteRootURL URLByAppendingPathComponent:self.primaryRemoteDatabaseName];
    [self createRemoteDatabase:self.primaryRemoteDatabaseName instanceURL:self.remoteRootURL];

    self.replicatorFactory = [[CDTReplicatorFactory alloc] initWithDatastoreManager:self.factory];

}

- (void)tearDown
{
    // Tear-down code here.

    // Delete remote database, stop the replicator.
    [self deleteRemoteDatabase:self.primaryRemoteDatabaseName instanceURL:self.remoteRootURL];

    self.datastore = nil;

    self.replicatorFactory = nil;

    [super tearDown];
}

/**
 Create a new replicator, and wait for replication from the remote database to complete.
 */
-(CDTReplicator *) pullFromRemote {
    return [self pullFromRemoteWithFilter:nil params:nil];
}

-(CDTReplicator *) pullFromRemoteWithFilter:(NSString*)filterName params:(NSDictionary*)params
{
    CDTPullReplication *pull = [CDTPullReplication replicationWithSource:self.primaryRemoteDatabaseURL
                                                                  target:self.datastore];
    
    pull.filter = filterName;
    pull.filterParams = params;
    
    NSError *error;
    CDTReplicator *replicator =  [self.replicatorFactory oneWay:pull error:&error];
    XCTAssertNil(error, @"%@",error);
    XCTAssertNotNil(replicator, @"CDTReplicator is nil");
    
    NSLog(@"Replicating from %@", [pull.source absoluteString]);
    if (![replicator startWithError:&error]) {
        XCTFail(@"CDTReplicator -startWithError: %@", error);
    }
    
    while (replicator.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
        NSLog(@" -> %@", [CDTReplicator stringForReplicatorState:replicator.state]);
    }
    
    return replicator;
}


/**
 Create a new replicator, and wait for replication from the local database to complete.
 */
-(CDTReplicator *) pushToRemote {
    return [self pushToRemoteWithFilter:nil params:nil];
}

/**
 Create a new replicator, and wait for replication from the local database to complete.
 */
-(CDTReplicator *) pushToRemoteWithFilter:(CDTFilterBlock)filter params:(NSDictionary*)params{
    
    CDTPushReplication *push = [CDTPushReplication replicationWithSource:self.datastore
                                                                  target:self.primaryRemoteDatabaseURL];
    push.filter = filter;
    push.filterParams = params;
    
    NSError *error;
    CDTReplicator *replicator =  [self.replicatorFactory oneWay:push error:&error];
    XCTAssertNil(error, @"%@",error);
    XCTAssertNotNil(replicator, @"CDTReplicator is nil");
    
    NSLog(@"Replicating to %@", [self.primaryRemoteDatabaseURL absoluteString]);
    if (![replicator startWithError:&error]) {
        XCTFail(@"CDTReplicator -startWithError: %@", error);
    }
   
    while (replicator.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
        NSLog(@" -> %@", [CDTReplicator stringForReplicatorState:replicator.state]);
    }

    return replicator;
}


#pragma mark - Tests

/**
 Load up a local database with n_docs with a single rev, then push it to 
 the configured remote database.
 */
-(void)testPushLotsOfOneRevDocuments
{
    // Create docs in local store
    NSLog(@"Creating documents...");
    [self createLocalDocs:n_docs];
    XCTAssertEqual(self.datastore.documentCount, n_docs, @"Incorrect number of documents created");

    CDTReplicator *replicator = [self pushToRemote];

    [self assertRemoteDatabaseHasDocCount:[[NSNumber numberWithUnsignedInteger:n_docs] integerValue]
                              deletedDocs:0];

    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertTrue(same, @"Remote and local databases differ");

    XCTAssertEqual(n_docs, (NSUInteger)replicator.changesTotal, @"total number of changes mismatch");
    XCTAssertEqual(n_docs, (NSUInteger)replicator.changesProcessed, @"processed number of changes mismatch");
}

/**
 Load up a remote database with n_docs with a single rev, then pull it to
 the local datastore.
 */
-(void) testPullLotsOfOneRevDocuments {

//    NSError *error;

    // Create docs in remote database
    NSLog(@"Creating documents...");

    [self createRemoteDocs:n_docs];

    CDTReplicator *replicator = [self pullFromRemote];

    XCTAssertEqual(self.datastore.documentCount, n_docs, @"Incorrect number of documents created");

    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertTrue(same, @"Remote and local databases differ");
    
    XCTAssertEqual(n_docs, (NSUInteger)replicator.changesTotal, @"total number of changes mismatch");
    
    XCTAssertEqual(n_docs, (NSUInteger)replicator.changesProcessed, @"processed number of changes mismatch");
}

-(void) testPullErrorsWhenLocalDatabaseIsDeleted
{
    
    [self createRemoteDocs:n_docs];
    
    CDTPullReplication *pull = [CDTPullReplication replicationWithSource:self.primaryRemoteDatabaseURL
                                                                  target:self.datastore];
    
    NSError *error;
    CDTReplicator *replicator =  [self.replicatorFactory oneWay:pull error:&error];
    CDTTestReplicatorDelegateDeleteLocalDatastoreAfterStart *mydel =
                                            [[CDTTestReplicatorDelegateDeleteLocalDatastoreAfterStart alloc] init];
    mydel.databaseToDelete = self.datastore.name;
    mydel.dsManager = self.factory;
    
    replicator.delegate = mydel;
    
    error = nil;
    XCTAssertTrue([replicator startWithError:&error], @"CDTReplicator -startWithError: %@", error);
    
    while (replicator.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
        NSLog(@" -> %@", [CDTReplicator stringForReplicatorState:replicator.state]);
    }

    
    XCTAssertTrue(n_docs != (NSUInteger)replicator.changesTotal, @"changesTotal: %ld, n_docs %ld",
                 replicator.changesTotal, n_docs);
    
    XCTAssertTrue(n_docs != (NSUInteger)replicator.changesProcessed, @"changesProcessed: %ld, n_docs %ld",
                   replicator.changesProcessed, n_docs);
    
    XCTAssertEqual(replicator.state, CDTReplicatorStateError, @"Found: %@, expected: (%@)",
                   [CDTReplicator stringForReplicatorState:replicator.state],
                   [CDTReplicator stringForReplicatorState:CDTReplicatorStateError]);

    XCTAssertEqual(mydel.error.code, CDTReplicatorErrorLocalDatabaseDeleted,
                   @"Wrong error code: %ld", mydel.error.code);
    
    //have to wait for the threadsto completely stop executing
    while(replicator.threadExecuting) {
        [NSThread sleepForTimeInterval:1.0f];
    }

    XCTAssertFalse(replicator.threadExecuting, @"First replicator thread executing");
    XCTAssertTrue(replicator.threadFinished, @"First replicator thread NOT finished");
    XCTAssertTrue(replicator.threadCanceled, @"First replicator thread NOT canceled");
    
}

-(void) testPushErrorsWhenLocalDatabaseIsDeleted
{
    
    [self createLocalDocs:n_docs];
    
    CDTPushReplication *push = [CDTPushReplication replicationWithSource:self.datastore
                                                                  target:self.primaryRemoteDatabaseURL];
    
    NSError *error;
    CDTReplicator *replicator =  [self.replicatorFactory oneWay:push error:&error];
    CDTTestReplicatorDelegateDeleteLocalDatastoreAfterStart *mydel =
                                            [[CDTTestReplicatorDelegateDeleteLocalDatastoreAfterStart alloc] init];
    mydel.databaseToDelete = self.datastore.name;
    mydel.dsManager = self.factory;
    
    replicator.delegate = mydel;
    
    error = nil;
    if (![replicator startWithError:&error]) {
        XCTFail(@"CDTReplicator -startWithError: %@", error);
    }
    
    while (replicator.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
        NSLog(@" -> %@", [CDTReplicator stringForReplicatorState:replicator.state]);
    }
    
    
    XCTAssertTrue(n_docs != (NSUInteger)replicator.changesTotal, @"changesTotal: %ld, n_docs %ld",
                 replicator.changesTotal, n_docs);
    
    XCTAssertTrue(n_docs != (NSUInteger)replicator.changesProcessed, @"changesProcessed: %ld, n_docs %ld",
                 replicator.changesProcessed, n_docs);
    
    XCTAssertEqual(replicator.state, CDTReplicatorStateError, @"Found: %@, expected: (%@)",
                   [CDTReplicator stringForReplicatorState:replicator.state],
                   [CDTReplicator stringForReplicatorState:CDTReplicatorStateError]);

    XCTAssertEqual(mydel.error.code, CDTReplicatorErrorLocalDatabaseDeleted,
                   @"Wrong error code: %ld", mydel.error.code);
    
    //have to wait for the threadsto completely stop executing
    while(replicator.threadExecuting) {
        [NSThread sleepForTimeInterval:1.0f];
    }
    
    XCTAssertFalse(replicator.threadExecuting, @"First replicator thread executing");
    XCTAssertTrue(replicator.threadFinished, @"First replicator thread NOT finished");
    XCTAssertTrue(replicator.threadCanceled, @"First replicator thread NOT canceled");
}

-(void) testSyncReplicationErrorsWhenLocalDatabaseDeleted_pushDelegateDeletes
{
    //create unique set of docs on local and remote databases.
    //set up both a push and pull replication and then effectively
    //cancel the replications by deleting the local database with one
    //of the replicator's delegates. then we check to ensure that
    //replicators quit as expected.
    //
    //this test uses the push replicator's delegate to delete the local
    //datastore
    
    //2000 docs should be sufficient to start a replication and delete the local
    //store before the replicators pull/push all of the remote/local docs
    //If this test fails because the replicators complete their job, then
    //increase increase the number of docs.
    [self createLocalDocs:2000];
    [self createRemoteDocs:2000 suffixFrom:2000];
    
    CDTPullReplication *pull = [CDTPullReplication replicationWithSource:self.primaryRemoteDatabaseURL
                                                                  target:self.datastore];
    CDTReplicator *pullReplicator =  [self.replicatorFactory oneWay:pull error:nil];
    CDTPushReplication *push = [CDTPushReplication replicationWithSource:self.datastore
                                                                  target:self.primaryRemoteDatabaseURL];
    CDTReplicator *pushReplicator =  [self.replicatorFactory oneWay:push error:nil];
    
    
    CDTTestReplicatorDelegateDeleteLocalDatastoreAfterStart *mydel =
    [[CDTTestReplicatorDelegateDeleteLocalDatastoreAfterStart alloc] init];
    mydel.databaseToDelete = self.datastore.name;
    mydel.dsManager = self.factory;
    
    pushReplicator.delegate = mydel;

    NSError *error;
    if (![pushReplicator startWithError:&error]) {
        XCTFail(@"CDTReplicator -startWithError: %@", error);
    }
    
    if (![pullReplicator startWithError:&error]) {
        XCTFail(@"CDTReplicator -startWithError: %@", error);
    }
    
    while (pushReplicator.isActive || pullReplicator.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
        NSLog(@"push -> %@", [CDTReplicator stringForReplicatorState:pushReplicator.state]);
        NSLog(@"pull -> %@", [CDTReplicator stringForReplicatorState:pullReplicator.state]);
    }
    
    
    //check pull replicator
    XCTAssertTrue(n_docs != (NSUInteger)pullReplicator.changesTotal, @"changesTotal: %ld, n_docs %ld",
                 pullReplicator.changesTotal, n_docs);
    
    XCTAssertTrue(n_docs != (NSUInteger)pullReplicator.changesProcessed, @"changesProcessed: %ld, n_docs %ld",
                 pullReplicator.changesProcessed, n_docs);

    XCTAssertEqual(pullReplicator.state, CDTReplicatorStateError, @"Found: %@, expected: (%@)",
                   [CDTReplicator stringForReplicatorState:pullReplicator.state],
                   [CDTReplicator stringForReplicatorState:CDTReplicatorStateError]);
    
    XCTAssertEqual(mydel.error.code, CDTReplicatorErrorLocalDatabaseDeleted,
                   @"Wrong error code: %ld", mydel.error.code);
    
    //have to wait for the threadsto completely stop executing
    while(pullReplicator.threadExecuting) {
        [NSThread sleepForTimeInterval:1.0f];
    }
    
    XCTAssertFalse(pullReplicator.threadExecuting, @"Pull replicator thread executing");
    XCTAssertTrue(pullReplicator.threadFinished, @"Pull replicator thread NOT finished");
    XCTAssertTrue(pullReplicator.threadCanceled, @"Pull replicator thread NOT canceled");

    
    //check push replicator
    XCTAssertTrue(n_docs != (NSUInteger)pushReplicator.changesTotal, @"changesTotal: %ld, n_docs %ld",
                 pushReplicator.changesTotal, n_docs);
    
    XCTAssertTrue(n_docs != (NSUInteger)pushReplicator.changesProcessed, @"changesProcessed: %ld, n_docs %ld",
                 pushReplicator.changesProcessed, n_docs);
    
    XCTAssertEqual(pushReplicator.state, CDTReplicatorStateError, @"Found: %@, expected: (%@)",
                   [CDTReplicator stringForReplicatorState:pushReplicator.state],
                   [CDTReplicator stringForReplicatorState:CDTReplicatorStateError]);
    
    XCTAssertEqual(mydel.error.code, CDTReplicatorErrorLocalDatabaseDeleted,
                   @"Wrong error code: %ld", mydel.error.code);
    
    //have to wait for the threadsto completely stop executing
    while(pushReplicator.threadExecuting) {
        [NSThread sleepForTimeInterval:1.0f];
    }
    
    XCTAssertFalse(pushReplicator.threadExecuting, @"Push replicator thread executing");
    XCTAssertTrue(pushReplicator.threadFinished, @"Push replicator thread NOT finished");
    XCTAssertTrue(pushReplicator.threadCanceled, @"Push replicator thread NOT canceled");
    
    BOOL notSame = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertFalse(notSame, @"Remote and local databases are the same and should be different");
    
}


-(void) testSyncReplicationErrorsWhenLocalDatabaseDeleted_pullDelegateDeletes
{
    //create unique set of docs on local and remote databases.
    //set up both a push and pull replication and then effectively
    //cancel the replications by deleting the local database with one
    //of the replicator's delegates. then we check to ensure that
    //replicators quit as expected.
    //
    //this test uses the pull replicator's delegate to delete the local
    //datastore
    
    //2000 docs should be sufficient to start a replication and delete the local
    //store before the replicators pull/push all of the remote/local docs
    //If this test fails because the replicators complete their job, then
    //increase increase the number of docs.
    [self createLocalDocs:2000];
    [self createRemoteDocs:2000 suffixFrom:2000];

    CDTPullReplication *pull = [CDTPullReplication replicationWithSource:self.primaryRemoteDatabaseURL
                                                                  target:self.datastore];
    CDTReplicator *pullReplicator =  [self.replicatorFactory oneWay:pull error:nil];
    CDTPushReplication *push = [CDTPushReplication replicationWithSource:self.datastore
                                                                  target:self.primaryRemoteDatabaseURL];
    CDTReplicator *pushReplicator =  [self.replicatorFactory oneWay:push error:nil];
    
    
    CDTTestReplicatorDelegateDeleteLocalDatastoreAfterStart *mydel =
    [[CDTTestReplicatorDelegateDeleteLocalDatastoreAfterStart alloc] init];
    mydel.databaseToDelete = self.datastore.name;
    mydel.dsManager = self.factory;
    
    pullReplicator.delegate = mydel;
    
    NSError *error;
    if (![pushReplicator startWithError:&error]) {
        XCTFail(@"CDTReplicator -startWithError: %@", error);
    }
    
    if (![pullReplicator startWithError:&error]) {
        XCTFail(@"CDTReplicator -startWithError: %@", error);
    }
    
    while (pushReplicator.isActive || pullReplicator.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
        NSLog(@"push -> %@", [CDTReplicator stringForReplicatorState:pushReplicator.state]);
        NSLog(@"pull -> %@", [CDTReplicator stringForReplicatorState:pullReplicator.state]);
    }
    
    
    //check pull replicator
    XCTAssertTrue(n_docs != (NSUInteger)pullReplicator.changesTotal, @"changesTotal: %ld, n_docs %ld",
                 pullReplicator.changesTotal, n_docs);
    
    XCTAssertTrue(n_docs != (NSUInteger)pullReplicator.changesProcessed, @"changesProcessed: %ld, n_docs %ld",
                 pullReplicator.changesProcessed, n_docs);
    
    XCTAssertEqual(pullReplicator.state, CDTReplicatorStateError, @"Found: %@, expected: (%@)",
                   [CDTReplicator stringForReplicatorState:pullReplicator.state],
                   [CDTReplicator stringForReplicatorState:CDTReplicatorStateError]);
    
    XCTAssertEqual(mydel.error.code, CDTReplicatorErrorLocalDatabaseDeleted,
                   @"Wrong error code: %ld", mydel.error.code);
    
    //have to wait for the threadsto completely stop executing
    while(pullReplicator.threadExecuting) {
        [NSThread sleepForTimeInterval:1.0f];
    }
    
    XCTAssertTrue(pullReplicator.threadFinished, @"Pull replicator thread NOT finished");
    XCTAssertTrue(pullReplicator.threadCanceled, @"Pull replicator thread NOT canceled");
    
    
    //check push replicator
    XCTAssertTrue(n_docs != (NSUInteger)pushReplicator.changesTotal, @"changesTotal: %ld, n_docs %ld",
                 pushReplicator.changesTotal, n_docs);
    
    XCTAssertTrue(n_docs != (NSUInteger)pushReplicator.changesProcessed, @"changesProcessed: %ld, n_docs %ld",
                 pushReplicator.changesProcessed, n_docs);
    
    XCTAssertEqual(pushReplicator.state, CDTReplicatorStateError, @"Found: %@, expected: (%@)",
                   [CDTReplicator stringForReplicatorState:pushReplicator.state],
                   [CDTReplicator stringForReplicatorState:CDTReplicatorStateError]);
    
    XCTAssertEqual(mydel.error.code, CDTReplicatorErrorLocalDatabaseDeleted,
                   @"Wrong error code: %ld", mydel.error.code);
    
    //have to wait for the threadsto completely stop executing
    while(pushReplicator.threadExecuting) {
        [NSThread sleepForTimeInterval:1.0f];
    }
    
    XCTAssertTrue(pushReplicator.threadFinished, @"Push replicator thread NOT finished");
    XCTAssertTrue(pushReplicator.threadCanceled, @"Push replicator thread NOT canceled");
    
    BOOL notSame = [self compareDatastore:self.datastore
                             withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertFalse(notSame, @"Remote and local databases are the same and should be different");
    
}



/**
 As per testPullLotsOfOneRevDocuments but ensuring indexes are updated.
 NB this currently about twice as slow as without indexing.
 */
-(void) testPullLotsOfOneRevDocumentsIndexed {
    
    NSError *error;
    
    // set up indexing
    CDTIndexManager *im = [[CDTIndexManager alloc] initWithDatastore:self.datastore error:&error];
    [im ensureIndexedWithIndexName:@"hello" fieldName:@"hello" error:&error];
    
    // Create docs in remote database
    NSLog(@"Creating documents...");
    
    [self createRemoteDocs:n_docs];
    
    [self pullFromRemote];
    
    XCTAssertEqual(self.datastore.documentCount, n_docs, @"Incorrect number of documents created");
    
    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertTrue(same, @"Remote and local databases differ");
    
    CDTQueryResult *res = [im queryWithDictionary:@{@"hello":@"world"} error:&error];
    XCTAssertEqual([[res documentIds] count], n_docs, @"Index does not return correct count");
}

-(void) testPullFilteredReplication {
    
    // Create docs in remote database
    NSLog(@"Creating documents...");
    
    int ndocs = 50; //don't need 100k docs
    
    [self createRemoteDocs:ndocs];
    
    //create remote filter
    NSString *ddoc = [NSString stringWithFormat:@"ddoc-%@",
                      [CloudantReplicationBase generateRandomString:5]];
    NSString *filterKey = @"docnum_by_range";
    NSString *filterName = [ddoc stringByAppendingFormat:@"/%@", filterKey];
    
    NSString *filterFunction = @"function(doc, req) {"
                               @"  if (doc.docnum >= req.query.min && doc.docnum < req.query.max)"
                               @"    return true;"
                               @"  else return false;"
                               @"}";
    
    NSDictionary *body = @{@"filters": @{ filterKey:filterFunction}};
    
    [self createRemoteDocWithId:[NSString stringWithFormat:@"_design/%@", ddoc] body:body];
    
    
    //replicate over a few individial docs - the filter range is [min, max).
    [self pullFromRemoteWithFilter:filterName params:@{@"min":@1, @"max":@2}];
    [self pullFromRemoteWithFilter:filterName params:@{@"min":@3, @"max":@4}];
    [self pullFromRemoteWithFilter:filterName params:@{@"min":@13, @"max":@14}];
    [self pullFromRemoteWithFilter:filterName params:@{@"min":@23, @"max":@24}];
    
    unsigned int totalReplicated = 4;
    
    //check for each doc
    NSArray *docids = @[[NSString stringWithFormat:@"doc-%i", 1],
                        [NSString stringWithFormat:@"doc-%i", 3],
                        [NSString stringWithFormat:@"doc-%i", 13],
                        [NSString stringWithFormat:@"doc-%i", 23]];
    
    NSArray *localDocs = [self.datastore getDocumentsWithIds:docids];
    XCTAssertNotNil(localDocs, @"nil");
    XCTAssertTrue(localDocs.count == totalReplicated, @"unexpected number of docs: %@",localDocs.count);
    XCTAssertTrue(self.datastore.documentCount == totalReplicated,
                 @"Incorrect number of documents created %lu", self.datastore.documentCount);
    
}

-(void) testPushFilteredReplicationAllowOne {
    
    // Create docs in local store
    [self createLocalDocs:10];
    
    CDTFilterBlock myFilter = ^BOOL(CDTDocumentRevision *rev, NSDictionary *param){
        return [[rev body][@"docnum"] isEqual:param[@"pickme"]];
    };
    
    [self pushToRemoteWithFilter:myFilter params:@{@"pickme":@3}];
    
    [self assertRemoteDatabaseHasDocCount:1
                              deletedDocs:0];

    //make sure the remote database has the appropriate document
    NSURL *docURL = [self.primaryRemoteDatabaseURL URLByAppendingPathComponent:@"doc-3"];
    NSDictionary* headers = @{@"accept": @"application/json",
                              @"content-type": @"application/json"};
    UNIHTTPJsonResponse *response = [[UNIRest get:^(UNISimpleRequest* request) {
        [request setUrl:[docURL absoluteString]];
        [request setHeaders:headers];
    }] asJson];
    NSDictionary *jsonResponse = response.body.object;
    XCTAssertTrue([jsonResponse[@"_id"] isEqual:@"doc-3"], @"%@", jsonResponse);
    XCTAssertTrue([jsonResponse[@"docnum"] isEqual:@3], @"%@", jsonResponse);
    
}

-(void) testPushFilteredReplicationAllowSome {
    
    // Create docs in local store
    [self createLocalDocs:10];
    
    CDTFilterBlock myFilter = ^BOOL(CDTDocumentRevision *rev, NSDictionary *param){
        NSInteger docNum = [[rev body][@"docnum"] integerValue];
        NSInteger threshold = [param[@"threshold"] integerValue];
        return docNum > threshold;
    };
    
    [self pushToRemoteWithFilter:myFilter params:@{@"threshold":@3}];
    
    [self assertRemoteDatabaseHasDocCount:7
                              deletedDocs:0];
    
}

-(void) testPushStateAfterStopping
{
    // Create docs in local store
    int nlocalDocs = 5000;
    [self createLocalDocs:nlocalDocs];

    CDTPushReplication *push = [CDTPushReplication replicationWithSource:self.datastore
                                                                  target:self.primaryRemoteDatabaseURL];
    
    CDTReplicator *replicator =  [self.replicatorFactory oneWay:push error:nil];
    
    CDTTestReplicatorDelegateStopAfterStart *myDelegate = [[CDTTestReplicatorDelegateStopAfterStart alloc] init];
    replicator.delegate = myDelegate;
    
    NSError *error;
    if (![replicator startWithError:&error]) {
        XCTFail(@"CDTReplicator -startWithError: %@", error);
    }
    
    while (replicator.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
    }

    XCTAssertEqual(replicator.state, CDTReplicatorStateStopped, @"expected a different state: %d (%@)",
                   replicator.state, [CDTReplicator stringForReplicatorState:replicator.state]);
    
    BOOL docComparison = [self compareDocCount:self.datastore
      expectFewerDocsInRemoteDatabase:self.primaryRemoteDatabaseURL];
    
    XCTAssertTrue(docComparison, @"Remote database doesn't have fewer docs than local.");
    
    //have to wait for the threadsto completely stop executing
    while(replicator.threadExecuting) {
        [NSThread sleepForTimeInterval:1.0f];
    }
    
    XCTAssertFalse(replicator.threadExecuting, @"First replicator thread executing");
    XCTAssertTrue(replicator.threadFinished, @"First replicator thread NOT finished");
    XCTAssertTrue(replicator.threadCanceled, @"First replicator thread NOT canceled");
    
}

/**
 Push a document with largeRevTreeSize revisions (>1000).
 */
-(void) testPushLargeRevTree {

    // Create the initial rev
    NSString *docId = @"doc-0";
    [self createLocalDocWithId:docId revs:largeRevTreeSize];
    XCTAssertEqual(self.datastore.documentCount, (NSUInteger)1, @"Incorrect number of documents created");

    [self pushToRemote];

    // Check document count in the remote DB
    [self assertRemoteDatabaseHasDocCount:1
                              deletedDocs:0];

    // Check number of revs
    NSURL *docURL = [self.primaryRemoteDatabaseURL URLByAppendingPathComponent:docId];
    NSDictionary* headers = @{@"accept": @"application/json",
                              @"content-type": @"application/json"};
    UNIHTTPJsonResponse *response = [[UNIRest get:^(UNISimpleRequest* request) {
        [request setUrl:[docURL absoluteString]];
        [request setHeaders:headers];
        [request setParameters:@{@"revs": @"true"}];
    }] asJson];
    NSDictionary *jsonResponse = response.body.object;

    // default couchdb revs_limit is 1000
    XCTAssertEqual([jsonResponse[@"_revisions"][@"ids"] count], (NSUInteger)1000, @"Wrong number of revs");
    XCTAssertTrue([jsonResponse[@"_rev"] hasPrefix:@"1500"], @"Not all revs seem to be replicated");

    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertTrue(same, @"Remote and local databases differ");
}

/**
 Pull a document with largeRevTreeSize revisions (>1000).
 */
-(void) testPullLargeRevTree {
    NSError *error;

    // Create the initial rev in remote datastore
    NSString *docId = [NSString stringWithFormat:@"doc-0"];

    [self createRemoteDocWithId:docId revs:largeRevTreeSize];

    [self pullFromRemote];

    CDTDocumentRevision *rev = [self.datastore getDocumentWithId:docId
                                                           error:&error];
    XCTAssertNil(error, @"Error getting replicated doc: %@", error);
    XCTAssertNotNil(rev, @"Error creating doc: rev was nil, but so was error");

    XCTAssertTrue([rev.revId hasPrefix:@"1500"], @"Unexpected current rev in local document");

    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertTrue(same, @"Remote and local databases differ");
}

/**
 Create n_docs remote documents and pull them into the local datastore. Then
 modify all document with ten revisions. Finally push the changes back and check
 the local and remote databases still match.
 */
-(void) testPullModifySeveralRevsPush
{
    NSError *error;
    NSInteger n_mods = 10;

    // Create docs in remote database
    NSLog(@"Creating documents...");
    [self createRemoteDocs:n_docs];
    [self pullFromRemote];
    XCTAssertEqual(self.datastore.documentCount, n_docs, @"Incorrect number of documents created");

    // Modify all the docs -- we know they're going to be doc-1 to doc-<n_docs+1>
    for (int i = 1; i < n_docs+1; i++) {
        NSString *docId = [NSString stringWithFormat:@"doc-%i", i];
        CDTDocumentRevision *rev = [self.datastore getDocumentWithId:docId error:&error];
        XCTAssertNil(error, @"Couldn't get document");
        [self addRevsToDocumentRevision:rev count:n_mods];
    }

    // Replicate the changes
    [self pushToRemote];

    [self assertRemoteDatabaseHasDocCount:n_docs
                              deletedDocs:0];

    // Check number of revs for all docs is <n_mods>
    NSDictionary* headers = @{@"accept": @"application/json",
                              @"content-type": @"application/json"};
    for (int i = 1; i < n_docs+1; i++) {
        NSString *docId = [NSString stringWithFormat:@"doc-%i", i];
        NSURL *docURL = [self.primaryRemoteDatabaseURL URLByAppendingPathComponent:docId];
        UNIHTTPJsonResponse *response = [[UNIRest get:^(UNISimpleRequest* request) {
            [request setUrl:[docURL absoluteString]];
            [request setHeaders:headers];
            [request setParameters:@{@"revs": @"true"}];
        }] asJson];
        NSDictionary *jsonResponse = response.body.object;

        XCTAssertEqual([jsonResponse[@"_revisions"][@"ids"] count], (NSUInteger)n_mods, @"Wrong number of revs");
        XCTAssertTrue([jsonResponse[@"_rev"] hasPrefix:@"10"], @"Not all revs seem to be replicated");
    }


    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertTrue(same, @"Remote and local databases differ");
}


/**
 Create n_docs remote documents and pull them into the local datastore. Then
 delete all the documents in the local database. Finally push the changes back and check
 the local and remote databases still match.
 */
-(void) testPullDeleteAllPush
{
    NSError *error;

    // Create docs in remote database
    NSLog(@"Creating documents...");
    [self createRemoteDocs:n_docs];
    [self pullFromRemote];
    XCTAssertEqual(self.datastore.documentCount, n_docs, @"Incorrect number of documents created");

    // Modify all the docs -- we know they're going to be doc-1 to doc-<n_docs+1>
    for (int i = 1; i < n_docs+1; i++) {
        NSString *docId = [NSString stringWithFormat:@"doc-%i", i];
        CDTDocumentRevision *rev = [self.datastore getDocumentWithId:docId error:&error];

        [self.datastore deleteDocumentFromRevision:rev error:&error];
        XCTAssertNil(error, @"Couldn't delete document");
    }

    // Replicate the changes
    [self pushToRemote];

    [self assertRemoteDatabaseHasDocCount:0
                              deletedDocs:n_docs];


    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertTrue(same, @"Remote and local databases differ");
}

/**
 Fire up two threads:
 
 1. Push revisions to the remote database so long as there are still changes
    in the local database.
 2. Create n_docs single-rev docs in the local datastore.
 
 This tests that the replicator can keep up with a database that's adding docs
 underneath it.
 */
-(void) test_pushDocsAsWritingThem
{
    TRVSMonitor *monitor = [[TRVSMonitor alloc] initWithExpectedSignalCount:2];
    [self performSelectorInBackground:@selector(pushDocsAsWritingThem_pullReplicateThenSignal:)
                           withObject:monitor];
    [self performSelectorInBackground:@selector(pushDocsAsWritingThem_populateLocalDatabaseThenSignal:)
                           withObject:monitor];

    [monitor wait];

    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertTrue(same, @"Remote and local databases differ");
}

-(void) pushDocsAsWritingThem_pullReplicateThenSignal:(TRVSMonitor*)monitor
{
    NSInteger count;
    do {
        [self pushToRemote];
        NSDictionary *dbMeta = [self remoteDbMetadata];
        count = [dbMeta[@"doc_count"] integerValue];
        NSLog(@"Remote count: %ld", (long)count);
    } while (count < n_docs);

    [monitor signal];
}

-(void) pushDocsAsWritingThem_populateLocalDatabaseThenSignal:(TRVSMonitor*)monitor
{
    [self createLocalDocs:n_docs];
    XCTAssertEqual(self.datastore.documentCount, n_docs, @"Incorrect number of documents created");
    [monitor signal];
}

/**
 Create n_docs in the remote database.

 Fire up two threads:

 1. Pull all revisions from the remote database.
 2. Create n_docs single-rev docs in the local datastore, with names that DON'T
    conflict with the ones being pulled.

 This tests that we can add documents concurrently with a replication.
 */
-(void) test_pullDocsWhileWritingOthers
{
    [self createRemoteDocs:n_docs];

    TRVSMonitor *monitor = [[TRVSMonitor alloc] initWithExpectedSignalCount:2];

    // Replicate n_docs from remote
    [self performSelectorInBackground:@selector(pullDocsWhileWritingOthers_pullReplicateThenSignal:)
                           withObject:monitor];

    // Create documents that don't conflict as we pull
    [self performSelectorInBackground:@selector(pullDocsWhileWritingOthers_populateLocalDatabaseThenSignal:)
                           withObject:monitor];

    [monitor wait];

    XCTAssertEqual(self.datastore.documentCount, (NSUInteger)n_docs*2, @"Wrong number of local docs");

    [self pushToRemote];

    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertTrue(same, @"Remote and local databases differ");
}

-(void) pullDocsWhileWritingOthers_pullReplicateThenSignal:(TRVSMonitor*)monitor
{
    [self pullFromRemote];
    [monitor signal];
}

-(void) pullDocsWhileWritingOthers_populateLocalDatabaseThenSignal:(TRVSMonitor*)monitor
{
    [self createLocalDocs:n_docs suffixFrom:n_docs+1];
    [monitor signal];
}

/**
 See test_pullDocsWhileWritingOthers.

 Test replicating all the documents to a third database to make sure we replicate
 both documents added to the local DB via local modifications and replication.
 */
-(void) test_pullDocsWhileWritingOthersWriteToThirdDB
{
    [self createRemoteDocs:n_docs];

    TRVSMonitor *monitor = [[TRVSMonitor alloc] initWithExpectedSignalCount:2];

    // Replicate n_docs from remote
    [self performSelectorInBackground:@selector(pullDocsWhileWritingOthers_pullReplicateThenSignal:)
                           withObject:monitor];

    // Create documents that don't conflict as we pull
    [self performSelectorInBackground:@selector(pullDocsWhileWritingOthers_populateLocalDatabaseThenSignal:)
                           withObject:monitor];

    [monitor wait];


    // Push to a third database and check against it.
    NSString *thirdDatabaseName = [NSString stringWithFormat:@"%@-test-third-database-%@",
                                   self.remoteDbPrefix,
                                   [CloudantReplicationBase generateRandomString:5]];

    [self createRemoteDatabase:thirdDatabaseName instanceURL:self.remoteRootURL];

    NSURL *thirdDatabase = [self.remoteRootURL URLByAppendingPathComponent:thirdDatabaseName];

    CDTPushReplication *push = [CDTPushReplication replicationWithSource:self.datastore
                                                                  target:thirdDatabase];
    
    CDTReplicator *replicator = [self.replicatorFactory oneWay:push error:nil];

    NSLog(@"Replicating to %@", [thirdDatabase absoluteString]);
    NSError *error;
    if (![replicator startWithError:&error]) {
        XCTFail(@"CDTReplicator -startWithError: %@", error);
    }
    
    while (replicator.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
        NSLog(@" -> %@", [CDTReplicator stringForReplicatorState:replicator.state]);
    }

    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:thirdDatabase];
    XCTAssertTrue(same, @"Remote and local databases differ");

    [self deleteRemoteDatabase:thirdDatabaseName instanceURL:self.remoteRootURL];
}

/**
 Create n_docs in the remote database.

 Fire up two threads:

 1. Pull all revisions from the remote database.
 2. Create documents in the local database with the same name, in reverse order.

 This tests that we can replicate while modifying the same documents locally.
 The docs are created in reverse order locally so we end up with some docs
 conflicted by local modifications, some by remote. In the end all docs
 will be conflicted.
 */
-(void) test_pullDocsWhileWritingSame
{
    [self createLocalDocs:n_docs suffixFrom:0 reverse:NO updates:NO];
    [self createRemoteDocs:n_docs];

    TRVSMonitor *monitor = [[TRVSMonitor alloc] initWithExpectedSignalCount:2];

    // Replicate n_docs from remote
    [self performSelectorInBackground:@selector(pullDocsWhileWritingSame_pullReplicateThenSignal:)
                           withObject:monitor];

    // Create documents that don't conflict as we pull
    [self performSelectorInBackground:@selector(pullDocsWhileWritingSame_populateLocalDatabaseThenSignal:)
                           withObject:monitor];

    [monitor wait];

    [self pushToRemote];

    XCTAssertEqual(self.datastore.documentCount, (NSUInteger)n_docs, @"Wrong number of local docs");

    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertTrue(same, @"Remote and local databases differ");
}

-(void) pullDocsWhileWritingSame_pullReplicateThenSignal:(TRVSMonitor*)monitor
{
    [self pullFromRemote];
    [monitor signal];
}

-(void) pullDocsWhileWritingSame_populateLocalDatabaseThenSignal:(TRVSMonitor*)monitor
{
    // Write in reverse so we'll definitely cross-streams with the concurrent
    // pull replication at some point.
    [self createLocalDocs:n_docs suffixFrom:0 reverse:YES updates:YES];
    [monitor signal];
}

/**
 See test_pullDocsWhileWritingSame.
 
 This test makes sure that we can replicate all the docs and conflicts
 to a third database.
 */
-(void) test_pullDocsWhileWritingSameWriteToThirdDB
{
    [self createLocalDocs:n_docs suffixFrom:0 reverse:NO updates:NO];
    [self createRemoteDocs:n_docs];

    TRVSMonitor *monitor = [[TRVSMonitor alloc] initWithExpectedSignalCount:2];

    // Replicate n_docs from remote
    [self performSelectorInBackground:@selector(pullDocsWhileWritingSame_pullReplicateThenSignal:)
                           withObject:monitor];

    // Create documents that don't conflict as we pull
    [self performSelectorInBackground:@selector(pullDocsWhileWritingSame_populateLocalDatabaseThenSignal:)
                           withObject:monitor];

    [monitor wait];


    // Push to a third database and check against it.
    NSString *thirdDatabaseName = [NSString stringWithFormat:@"%@-test-third-database-%@",
                                      self.remoteDbPrefix,
                                      [CloudantReplicationBase generateRandomString:5]];

    [self createRemoteDatabase:thirdDatabaseName instanceURL:self.remoteRootURL];

    NSURL *thirdDatabase = [self.remoteRootURL URLByAppendingPathComponent:thirdDatabaseName];

    CDTPushReplication *push = [CDTPushReplication replicationWithSource:self.datastore
                                                                  target:thirdDatabase];
    
    CDTReplicator *replicator = [self.replicatorFactory oneWay:push error:nil];
    
    NSLog(@"Replicating to %@", [thirdDatabase absoluteString]);
    NSError *error;
    if (![replicator startWithError:&error]) {
        XCTFail(@"CDTReplicator -startWithError: %@", error);
    }
    
    while (replicator.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
        NSLog(@" -> %@", [CDTReplicator stringForReplicatorState:replicator.state]);
    }

    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:thirdDatabase];
    XCTAssertTrue(same, @"Remote and local databases differ");

    [self deleteRemoteDatabase:thirdDatabaseName instanceURL:self.remoteRootURL];
}


/**
 Reproduce reported issue:
 
 1. Create remote database
 2. Add 250 docs of type A to remote database, 10 docs of type B.
 3. Remove all type A documents from remote database.
 4. Launch application. (test doesn't need to do this)
 5. Create new local database.
 6. Pull and push replicate from remote database to local database; wait until complete.
 7. Add 100 documents to local database of type A.
 8. Push replicate local to remote -> push replication fails
 
 I was able to reduce a couple of things:
 
  - The different "types" of documents was not important.
  - Number of removed documents doesn't matter.
  - Important thing is resurrecting deleted documents and pushing.
 */
-(void) test_CreateRemoteDeleteRemotePullPushCreateLocalPush
{
    // Create docs in remote database
    NSLog(@"Creating documents...");
    
    NSInteger startSuffix = 0;
    NSInteger remoteTotal = 10;
    NSInteger remoteDeleteCount = 5;
    NSInteger localTotal = 1;
    
    [self createRemoteDocs:remoteTotal suffixFrom:startSuffix];
    
    // Modify all the docs -- we know they're going to be doc-1 to doc-<n_docs+1>
    for (NSInteger i = startSuffix+1; i < remoteDeleteCount+1; i++) {
        NSString *docId = [NSString stringWithFormat:@"doc-%li", (long)i];
        [self deleteRemoteDocWithId:docId];
    }
    
    [self pullFromRemote];
    [self pushToRemote];
    
    // These will have the same name as the replicated deleted docs.
    [self createLocalDocs:localTotal suffixFrom:startSuffix];
    
    // This should no longer fail
    [self pushToRemote];
    
    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertTrue(same, @"Remote and local databases differ");
}


/**
 Test that we can replicate resurrected documents. 
 
 This is a minimal example of the bug in test_CreateRemoteDeleteRemotePullPushCreateLocalPush.
 */
-(void) test_PushResurectedDocument
{
    [self createLocalDocs:1 suffixFrom:0];
    
    CDTDocumentRevision *rev = [self.datastore getDocumentWithId:@"doc-1" error:nil];
    [self.datastore deleteDocumentFromRevision:rev error:nil];
    
    [self createLocalDocs:1 suffixFrom:0];
    
    // This should fail
    [self pushToRemote];
    
    BOOL same = [self compareDatastore:self.datastore
                          withDatabase:self.primaryRemoteDatabaseURL];
    XCTAssertTrue(same, @"Remote and local databases differ");
}

/**
 Test that we can successfully add extra headers to HTTP requests. 
 */
-(void)testCreateReplicationWithExtraHeaders
{
    
    [self createRemoteDocs:100];
    
    CDTPullReplication *pull = [CDTPullReplication replicationWithSource:self.primaryRemoteDatabaseURL
                                                                  target:self.datastore];

    
    NSString *userAgent = [NSString stringWithFormat:@"%@/testCreateReplicationWithExtraHeaders",
                           [CDTAbstractReplication defaultUserAgentHTTPHeader]];
    NSDictionary *extraHeaders = @{@"SpecialHeader": @"foo", @"user-agent":userAgent};
    pull.optionalHeaders = extraHeaders;
    
    NSDictionary *pullDoc = [pull dictionaryForReplicatorDocument:nil];
    XCTAssertEqualObjects(pullDoc[@"headers"][@"SpecialHeader"], @"foo", @"Bad headers: %@",
                         pullDoc[@"headers"]);
    
    XCTAssertEqualObjects(pullDoc[@"headers"][@"user-agent"], userAgent, @"Bad headers: %@",
                         pullDoc[@"headers"]);

    
    ReplicatorURLProtocolTester* tester = [[ReplicatorURLProtocolTester alloc] init];

    tester.expectedHeaders = extraHeaders;
    [ReplicatorURLProtocol setTestDelegate:tester];

    [NSURLProtocol registerClass:[ReplicatorURLProtocol class]];

    CDTReplicator *replicator =  [self.replicatorFactory oneWay:pull error:nil];
    
    [replicator startWithError:nil];
    
    while (replicator.isActive) {
        [NSThread sleepForTimeInterval:0.5f];
        NSLog(@" -> %@", [CDTReplicator stringForReplicatorState:replicator.state]);
    }
    
    [NSURLProtocol unregisterClass:[ReplicatorURLProtocol class]];
    [ReplicatorURLProtocol setTestDelegate:nil];
    
    XCTAssertNil(tester.headerFailures, @"Errors found in headers.");
    
    for (NSString *headerName in tester.headerFailures) {
        
        XCTAssertTrue([tester.headerFailures[headerName] integerValue] == [@0 integerValue],
                     @"Found %ld failures with the header \"%@\"", 
                     [tester.headerFailures[headerName] integerValue], headerName);
        
    }
}

-(void) testMultiThreadedReplication
{
    CDTPullReplication *pull = [CDTPullReplication replicationWithSource:self.primaryRemoteDatabaseURL
                                                                  target:self.datastore];
    CDTReplicator *replicator =  [self.replicatorFactory oneWay:pull error:nil];
    
    CDTDatastore *secondDatastore = [self.factory datastoreNamed:@"test2" error:nil];
    CDTPullReplication *secondPull = [CDTPullReplication replicationWithSource:self.primaryRemoteDatabaseURL
                                                                  target:secondDatastore];
    CDTReplicator *secondReplicator =  [self.replicatorFactory oneWay:secondPull error:nil];
    
    [self createRemoteDocs:2000];
    
    CDTTestReplicatorMultiThreaded *tester = [[CDTTestReplicatorMultiThreaded alloc] init];
    tester.firstReplicator = replicator;
    tester.secondReplicator = secondReplicator;
    replicator.delegate = tester;
    secondReplicator.delegate = tester;
    
    XCTAssertFalse(replicator.threadExecuting, @"First replicator thread executing");
    XCTAssertFalse(replicator.threadFinished, @"First replicator thread finished");
    XCTAssertFalse(replicator.threadCanceled, @"First replicator thread canceled");

    XCTAssertFalse(secondReplicator.threadExecuting, @"Second replicator thread executing");
    XCTAssertFalse(secondReplicator.threadFinished, @"Second replicator thread finished");
    XCTAssertFalse(secondReplicator.threadCanceled, @"Second replicator thread canceled");
    
    NSError *error;
    XCTAssertTrue([replicator startWithError:&error],
                @"First replicator started with error: %@", error);
    error = nil;
    XCTAssertTrue([secondReplicator startWithError:&error],
                @"Second replicator started with error: %@", error);

    
    XCTAssertTrue(replicator.threadExecuting, @"First replicator thread NOT executing");
    XCTAssertFalse(replicator.threadFinished, @"First replicator thread finished");
    XCTAssertFalse(replicator.threadCanceled, @"First replicator thread canceled");
    
    XCTAssertTrue(secondReplicator.threadExecuting, @"Second replicator thread NOT executing");
    XCTAssertFalse(secondReplicator.threadFinished, @"Second replicator thread finished");
    XCTAssertFalse(secondReplicator.threadCanceled, @"Second replicator thread canceled");
    
    while (replicator.isActive || secondReplicator.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
        NSLog(@" 1st replicator -> %@", [CDTReplicator stringForReplicatorState:replicator.state]);
        NSLog(@"    changes Processed: %ld", replicator.changesProcessed);
        NSLog(@" 2nd replicator -> %@", [CDTReplicator stringForReplicatorState:secondReplicator.state]);
        NSLog(@"    changes Processed: %ld", secondReplicator.changesProcessed);
        
    }
    
    XCTAssertTrue(tester.multiThreaded, @"Delegate didn't find multithreading evidence.");

    //wait for the threads to completely stop executing
    while(replicator.threadExecuting || secondReplicator.threadExecuting) {
        [NSThread sleepForTimeInterval:1.0f];
    }

    XCTAssertFalse(replicator.threadExecuting, @"First replicator thread executing");
    XCTAssertTrue(replicator.threadFinished, @"First replicator thread NOT finished");
    XCTAssertFalse(replicator.threadCanceled, @"First replicator thread canceled");
    
    XCTAssertFalse(secondReplicator.threadExecuting, @"Second replicator thread executing");
    XCTAssertTrue(secondReplicator.threadFinished, @"Second replicator thread NOT finished");
    XCTAssertFalse(secondReplicator.threadCanceled, @"Second replicator thread canceled");
    
}

@end
