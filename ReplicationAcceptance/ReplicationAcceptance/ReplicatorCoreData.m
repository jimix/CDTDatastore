//
//  ReplicatorCoreData.m
//  ReplicationAcceptance
//
//  Created by Jimi Xenidis on 12/19/14.
//
//

#import <CoreData/CoreData.h>
#import <UNIRest.h>

#import "ReplicatorCoreData.h"
#import "CDTIncrementalStore.h"

@interface ReplicatorCoreData ()

@property (nonatomic, strong) NSManagedObjectModel *managedObjectModel;
@property (nonatomic, strong) NSManagedObjectModel *fromMom;
@property (nonatomic, strong) NSManagedObjectContext *managedObjectContext;
@property (nonatomic, strong) NSPersistentStoreCoordinator *persistentStoreCoordinator;
@property (nonatomic, strong) NSURL *storeURL;
@property (nonatomic, strong) NSURL *fromURL;
@property (nonatomic, strong) NSString *primaryRemoteDatabaseName;
@property (nonatomic, strong) NSMutableArray *secondaryRemoteDatabaseNames;
@property (nonatomic, strong) NSString *fromCDE;
@property (nonatomic, strong) NSString *toCDE;
@property (nonatomic, strong) NSMappingModel *mapper;

@end

@interface Entry : NSManagedObject
@property (nonatomic, strong) NSNumber *number;
@property (nonatomic, strong) NSString *string;
@property (nonatomic, strong) NSDate *created;
@property (nonatomic, strong) NSSet *stuff;
@end

@implementation Entry
@dynamic number, string, created, stuff;
@end

@interface Stuff : NSManagedObject
@property (nonatomic, retain) NSNumber *size;
@property (nonatomic, retain) NSString *data;
@property (nonatomic, retain) Entry *entry;
@end

@implementation Stuff
@dynamic size, data, entry;
@end

@implementation ReplicatorCoreData

- (Entry *)makeEntry:(NSManagedObjectContext *)moc
{
    Entry *e =
        [NSEntityDescription insertNewObjectForEntityForName:@"Entry" inManagedObjectContext:moc];
    XCTAssertNotNil(e, @"could not get entity");
    return e;
}

- (NSManagedObjectModel *)managedObjectModel
{
    if (_managedObjectModel != nil) {
        return _managedObjectModel;
    }

    NSBundle *bundle = [NSBundle bundleForClass:[self class]];
    NSURL *dir = [bundle URLForResource:@"CDE" withExtension:@"momd"];
    XCTAssertNotNil(dir, @"could not find CoreDataEntry resource directory");

    NSURL *toURL;
    if (self.toCDE) {
        toURL = [NSURL URLWithString:self.toCDE relativeToURL:dir];
    } else {
        // take the default defined by the directory
        toURL = dir;
    }
    XCTAssertNotNil(toURL, @"could not find CoreDataEntry model file");

    _managedObjectModel = [[NSManagedObjectModel alloc] initWithContentsOfURL:toURL];
    XCTAssertTrue(([[_managedObjectModel entities] count] > 0), @"no entities");

    if (self.fromCDE) {
        NSURL *fromURL = [NSURL URLWithString:self.fromCDE relativeToURL:dir];
        self.fromMom = [[NSManagedObjectModel alloc] initWithContentsOfURL:fromURL];
        XCTAssertNotNil(self.fromMom, @"Could not create from model");
    } else {
        self.fromMom = nil;
    }

    return _managedObjectModel;
}

- (NSManagedObjectContext *)managedObjectContext
{
    if (_managedObjectContext != nil) {
        return _managedObjectContext;
    }

    NSPersistentStoreCoordinator *coordinator = [self persistentStoreCoordinator];
    if (coordinator != nil) {
        _managedObjectContext = [NSManagedObjectContext new];
        [_managedObjectContext setPersistentStoreCoordinator:coordinator];
    }
    return _managedObjectContext;
}

- (NSURL *)createSecondaryDatabase:(NSString *)append
{
    XCTAssertTrue(([append length] > 0), @"append must no be empty");
    if (!self.secondaryRemoteDatabaseNames) {
        self.secondaryRemoteDatabaseNames = [NSMutableArray array];
    }

    NSString *sec = [self.primaryRemoteDatabaseName stringByAppendingString:append];
    NSURL *secURL = [NSURL URLWithString:sec relativeToURL:self.remoteRootURL];
    XCTAssertNotNil(secURL, "Secondary URL evaluated to nil?");

    [self createRemoteDatabase:sec instanceURL:self.remoteRootURL];

    [self.secondaryRemoteDatabaseNames addObject:sec];

    return secURL;
}

- (NSPersistentStoreCoordinator *)persistentStoreCoordinator
{
    if (_persistentStoreCoordinator != nil) {
        return _persistentStoreCoordinator;
    }

    NSManagedObjectModel *toMom = self.managedObjectModel;

    NSString *storeType;
    NSURL *rootURL;

    // quick hack to enable a known store type for testing
    const BOOL sql = NO;
    if (sql) {
        storeType = NSSQLiteStoreType;
        NSFileManager *fileManager = [NSFileManager defaultManager];
        rootURL = [[fileManager URLsForDirectory:NSDocumentDirectory
                                       inDomains:NSUserDomainMask] lastObject];
    } else {
        storeType = [CDTIncrementalStore type];
        rootURL = self.remoteRootURL;
    }

    NSError *err = nil;
    NSURL *storeURL;
    NSPersistentStore *theStore;

    if (self.fromMom) {
        NSError *err = nil;
        NSMappingModel *mapMom = [NSMappingModel inferredMappingModelForSourceModel:self.fromMom
                                                                   destinationModel:toMom
                                                                              error:&err];
        XCTAssertNotNil(mapMom, @"Failed to create mapping model");
        XCTAssertNil(err, @"Error: %@", err);

        NSURL *fromURL = [NSURL URLWithString:self.primaryRemoteDatabaseName relativeToURL:rootURL];

        storeURL = [self createSecondaryDatabase:@"-migrate"];

        err = nil;
        NSMigrationManager *mm =
            [[NSMigrationManager alloc] initWithSourceModel:self.fromMom destinationModel:toMom];
        XCTAssertNotNil(mm, @"Failed to create migration manager");

        XCTAssertTrue([mm migrateStoreFromURL:fromURL
                                         type:storeType
                                      options:nil
                             withMappingModel:mapMom
                             toDestinationURL:storeURL
                              destinationType:storeType
                           destinationOptions:nil
                                        error:&err],
                      @"migration failed");
        XCTAssertNil(err, @"error: %@", err);
    } else {
        storeURL = [NSURL URLWithString:self.primaryRemoteDatabaseName relativeToURL:rootURL];
    }

    _persistentStoreCoordinator =
        [[NSPersistentStoreCoordinator alloc] initWithManagedObjectModel:toMom];

    XCTAssertNotNil(_persistentStoreCoordinator, @"Failed to create PSC");

    // Since we perform all versioning manually...
    NSDictionary *options = @{ NSIgnorePersistentStoreVersioningOption : @(YES) };

    theStore = [_persistentStoreCoordinator addPersistentStoreWithType:storeType
                                                         configuration:nil
                                                                   URL:storeURL
                                                               options:options
                                                                 error:&err];
    XCTAssertNotNil(theStore, @"could not get theStore: %@", err);

    self.storeURL = storeURL;

    return _persistentStoreCoordinator;
}

- (CDTIncrementalStore *)getIncrementalStore
{
    NSArray *stores = [CDTIncrementalStore storesFromCoordinator:self.persistentStoreCoordinator];
    XCTAssertNotNil(stores, @"could not get stores");
    CDTIncrementalStore *store = [stores firstObject];
    XCTAssertNotNil(store, @"could not get incremental store");

    return store;
}

- (void)setUp
{
    [super setUp];

    // Create remote database
    self.primaryRemoteDatabaseName =
        [NSString stringWithFormat:@"%@-test-coredata-database-%@", self.remoteDbPrefix,
                                   [CloudantReplicationBase generateRandomString:5]];
    self.primaryRemoteDatabaseURL =
        [self.remoteRootURL URLByAppendingPathComponent:self.primaryRemoteDatabaseName];

    [self createRemoteDatabase:self.primaryRemoteDatabaseName instanceURL:self.remoteRootURL];
}

- (void)tearDown
{
    self.managedObjectContext = nil;
    self.persistentStoreCoordinator = nil;

    // Delete remote database
    [self deleteRemoteDatabase:self.primaryRemoteDatabaseName instanceURL:self.remoteRootURL];

    for (NSString *dbName in self.secondaryRemoteDatabaseNames) {
        [self deleteRemoteDatabase:dbName instanceURL:self.remoteRootURL];
    }
    [super tearDown];
}

- (NSManagedObjectContext *)createNumbersAndSave:(int)max
{
    NSError *err = nil;
    // This will create the database
    NSManagedObjectContext *moc = self.managedObjectContext;
    XCTAssertNotNil(moc, @"could not create Context");

    // create some entries
    for (int i = 0; i < max; i++) {
        Entry *e = [self makeEntry:moc];

        e.number = @(i);
        e.string = [NSString stringWithFormat:@"%u", (max * 10) + i];
        e.created = [NSDate dateWithTimeIntervalSinceNow:0];
    }

    // save to backing store
    XCTAssertTrue([moc save:&err], @"MOC save failed");
    XCTAssertNil(err, @"MOC save failed with error: %@", err);

    return moc;
}

- (void)removeLocalDatabase
{
    NSError *err = nil;

    /**
     *  blow away the local database
     */
    self.managedObjectContext = nil;
    self.persistentStoreCoordinator = nil;

    // remove the entire database directory
    NSURL *dir = [CDTIncrementalStore localDir];
    NSFileManager *fm = [NSFileManager defaultManager];
    XCTAssertNotNil(fm, @"Could not get File Manager");
    if (![fm removeItemAtURL:dir error:&err]) {
        XCTAssertTrue(err.code != NSFileNoSuchFileError,
                      @"removal of database directory failed: %@", err);
    }
}

- (NSUInteger)pushToURL:(NSURL *)url
{
    NSError *err = nil;
    CDTIncrementalStore *myIS = [self getIncrementalStore];
    XCTAssertNotNil(myIS, "Could not get IS Object");
    CDTReplicator *pusher = [myIS replicatorThatPushesToURL:url withError:&err];

    XCTAssertNotNil(pusher, @"Pusher create faile with: %@", err);

    XCTAssertTrue([pusher startWithError:&err], @"Push Failed with error: %@", err);
    while (pusher.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
    }
    return pusher.changesTotal;
}

- (NSUInteger)pushMe { return [self pushToURL:self.primaryRemoteDatabaseURL]; }

- (NSUInteger)pullFromURL:(NSURL *)url
{
    NSError *err = nil;
    CDTIncrementalStore *myIS = [self getIncrementalStore];
    XCTAssertNotNil(myIS, "Could not get IS Object");
    CDTReplicator *puller = [myIS replicatorThatPullsFromURL:url withError:&err];

    XCTAssertNotNil(puller, @"Puller create faile with: %@", err);

    XCTAssertTrue([puller startWithError:&err], @"Pull Failed with error: %@", err);
    while (puller.isActive) {
        [NSThread sleepForTimeInterval:1.0f];
    }
    return puller.changesTotal;
}

- (NSUInteger)pullMe { return [self pullFromURL:self.primaryRemoteDatabaseURL]; }

- (NSInteger)doConflictWithErr:(NSError **)error
{
    CDTIncrementalStore *myIS = [self getIncrementalStore];
    XCTAssertNotNil(myIS, "Could not get IS Object");

    return [myIS processConflictsWithError:error];
}

- (void)testCoreDataPushPull
{
    int max = 100;
    NSError *err = nil;

    NSManagedObjectContext *moc = [self createNumbersAndSave:max];

    // there is actually `max` docs plus the metadata document
    int docs = max + 1;

    /**
     *  Push
     */

    NSInteger count = [self pushMe];
    XCTAssertTrue(count == docs, @"push: unexpected processed objects: %@ != %d", @(count), docs);

    [self removeLocalDatabase];

    /**
     *  Out of band tally of the number of documents in the remote replicant
     */
    NSString *all_docs =
        [NSString stringWithFormat:@"%@/_all_docs?limit=0", [self.storeURL absoluteString]];
    UNIHTTPRequest *req = [UNIRest get:^(UNISimpleRequest *request) { [request setUrl:all_docs]; }];
    UNIHTTPJsonResponse *json = [req asJson];
    UNIJsonNode *body = json.body;
    NSDictionary *dic = body.object;
    NSNumber *total_rows = dic[@"total_rows"];
    count = [total_rows integerValue];
    XCTAssertTrue(count == docs, @"oob: unexpected number of objects: %@ != %d", @(count), docs);

    /**
     *  New context for pull
     */
    moc = self.managedObjectContext;
    XCTAssertNotNil(moc, @"could not create Context");

    count = [self pullMe];
    XCTAssertTrue(count == docs, @"pull: unexpected processed objects: %@ != %d", @(count), docs);

    /**
     *  Read it back
     */
    NSArray *results;
    NSSortDescriptor *sd = [NSSortDescriptor sortDescriptorWithKey:@"number" ascending:YES];

    NSFetchRequest *fr = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    fr.shouldRefreshRefetchedObjects = YES;
    fr.sortDescriptors = @[ sd ];

    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);
    count = [results count];
    XCTAssertTrue(count == max, @"fetch: unexpected processed objects: %@ != %d", @(count), max);

    long long last = -1;
    for (Entry *e in results) {
        long long val = [e.number longLongValue];
        XCTAssertTrue(val < max, @"entry is out of range [0, %d): %lld", max, val);
        XCTAssertTrue(val == last + 1, @"unexpected entry %@: %@", @(val), e);
        ++last;
    }
}

- (void)testCoreDataDuplication
{
    int max = 100;
    NSError *err = nil;

    NSManagedObjectContext *moc = [self createNumbersAndSave:max];

    // there is actually `max` docs plus the metadata document
    int docs = max + 1;

    // push
    NSInteger count = [self pushMe];
    XCTAssertTrue(count == docs, @"push: unexpected processed objects: %@ != %d", @(count), docs);

    [self removeLocalDatabase];

    // make another core data set with the exact same series
    moc = [self createNumbersAndSave:max];

    // now pull
    count = [self pullMe];
    XCTAssertTrue(count == docs, @"pull: unexpected processed objects: %@ != %d", @(count), docs);

    // Read it back
    NSArray *results;
    NSSortDescriptor *sd = [NSSortDescriptor sortDescriptorWithKey:@"number" ascending:YES];

    NSFetchRequest *fr = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    fr.shouldRefreshRefetchedObjects = YES;
    fr.sortDescriptors = @[ sd ];

    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);
    count = [results count];
    XCTAssertTrue(count == max * 2, @"fetch: unexpected processed objects: %@ != %d", @(count),
                  max * 2);

    // Find dupes
    // see:
    // https://developer.apple.com/library/ios/documentation/DataManagement/Conceptual/UsingCoreDataWithiCloudPG/UsingSQLiteStoragewithiCloud/UsingSQLiteStoragewithiCloud.html#//apple_ref/doc/uid/TP40013491-CH3-SW8

    /**
     *  1. Choose a property or a hash of multiple properties to use as a
     *     unique ID for each record.
     */
    NSString *uniquePropertyKey = @"number";
    NSExpression *countExpression =
        [NSExpression expressionWithFormat:@"count:(%@)", uniquePropertyKey];
    NSExpressionDescription *countExpressionDescription = [[NSExpressionDescription alloc] init];
    [countExpressionDescription setName:@"count"];
    [countExpressionDescription setExpression:countExpression];
    [countExpressionDescription setExpressionResultType:NSInteger64AttributeType];
    NSManagedObjectContext *context = moc;
    NSEntityDescription *entity =
        [NSEntityDescription entityForName:@"Entry" inManagedObjectContext:context];
    NSAttributeDescription *uniqueAttribute =
        [[entity attributesByName] objectForKey:uniquePropertyKey];

    /**
     *  2. Fetch the number of times each unique value appears in the store.
     *     The context returns an array of dictionaries, each containing
     *     a unique value and the number of times that value appeared in
     *     the store.
     */
    NSFetchRequest *fetchRequest = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    [fetchRequest setPropertiesToFetch:@[ uniqueAttribute, countExpressionDescription ]];
    [fetchRequest setPropertiesToGroupBy:@[ uniqueAttribute ]];
    [fetchRequest setResultType:NSDictionaryResultType];
    NSArray *fetchedDictionaries = [moc executeFetchRequest:fetchRequest error:&err];

    // check
    XCTAssertNotNil(fetchedDictionaries, @"fetch request failed: %@", err);
    count = [fetchedDictionaries count];
    XCTAssertTrue(count == max, @"fetch: unexpected processed objects: %@ != %d", @(count), max);

    /**
     *  3. Filter out unique values that have no duplicates.
     */
    NSMutableArray *valuesWithDupes = [NSMutableArray array];
    for (NSDictionary *dict in fetchedDictionaries) {
        NSNumber *count = dict[@"count"];
        if ([count integerValue] > 1) {
            [valuesWithDupes addObject:dict[@"number"]];
        }
    }

    /**
     *  4. Use a predicate to fetch all of the records with duplicates.
     *     Use a sort descriptor to properly order the results for the
     *     winner algorithm in the next step.
     */
    NSFetchRequest *dupeFetchRequest = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    [dupeFetchRequest setIncludesPendingChanges:NO];
    NSPredicate *predicate = [NSPredicate predicateWithFormat:@"number IN (%@)", valuesWithDupes];
    [dupeFetchRequest setPredicate:predicate];

    sd = [NSSortDescriptor sortDescriptorWithKey:@"number" ascending:YES];
    [dupeFetchRequest setSortDescriptors:@[ sd ]];
    NSArray *dupes = [moc executeFetchRequest:dupeFetchRequest error:&err];

    // check
    XCTAssertNotNil(dupes, @"fetch request failed: %@", err);
    count = [dupes count];
    XCTAssertTrue(count == max * 2, @"fetch: unexpected processed objects: %@ != %d", @(count),
                  max * 2);

    /**
     *  5. Choose the winner.
     *     After retrieving all of the duplicates, your app decides which
     *     ones to keep. This decision must be deterministic, meaning that
     *     every peer should always choose the same winner. Among other
     *     methods, your app could store a created or last-changed timestamp
     *     for each record and then decide based on that.
     */
    Entry *prevObject;
    for (Entry *duplicate in dupes) {
        if (prevObject) {
            if (duplicate.number == prevObject.number) {
                if ([duplicate.created compare:prevObject.created] == NSOrderedAscending) {
                    [moc deleteObject:duplicate];
                } else {
                    [moc deleteObject:prevObject];
                    prevObject = duplicate;
                }
            } else {
                prevObject = duplicate;
            }
        } else {
            prevObject = duplicate;
        }
    }
    /**
     *  Remember to set a batch size on the fetch and whenever you reach
     *  the end of a batch, save the context.
     */
    XCTAssertTrue([moc save:&err], @"MOC save failed");
    XCTAssertNil(err, @"MOC save failed with error: %@", err);

    // read it back
    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);
    count = [results count];
    XCTAssertTrue(count == max, @"fetch: unexpected processed objects: %@ != %d", @(count), max);

    count = [self pushMe];
    XCTAssertTrue(count == docs + max, @"push: unexpected processed objects: %@ != %d", @(count),
                  docs + max);
}

- (void)testCoreDataMigration
{
    int max = 10;
    NSError *err = nil;

    // force v1.0
    self.fromCDE = nil;
    self.toCDE = @"CDEv1.0.mom";

    NSManagedObjectContext *moc = [self createNumbersAndSave:max];

    // save it
    XCTAssertTrue([moc save:&err], @"MOC save failed");
    XCTAssertNil(err, @"MOC save failed with error: %@", err);

    /**
     *  Read it back
     */
    NSArray *results;

    NSFetchRequest *fr = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    fr.shouldRefreshRefetchedObjects = YES;

    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);
    NSInteger count = [results count];
    XCTAssertTrue(count == max, @"fetch: unexpected processed objects: %@ != %d", @(count), max);

    NSManagedObject *mo = [results firstObject];
    XCTAssertNotNil([mo valueForKey:@"number"]);
    XCTAssertThrows([mo valueForKey:@"checkit"]);

    // drop the store, I think I'm leaking here
    moc = nil;
    self.managedObjectModel = nil;
    self.managedObjectContext = nil;
    self.persistentStoreCoordinator = nil;

    // force v1.1
    self.fromCDE = self.toCDE;
    self.toCDE = @"CDEv1.1.mom";

    // bring it back with this object model
    moc = self.managedObjectContext;

    // Reload the fetch request becauses it caches info from the old model
    fr = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    fr.shouldRefreshRefetchedObjects = YES;

    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);
    count = [results count];
    XCTAssertTrue(count == max, @"fetch: unexpected processed objects: %@ != %d", @(count), max);

    mo = [results firstObject];
    XCTAssertNotNil([mo valueForKey:@"number"]);
    XCTAssertNoThrow([mo valueForKey:@"checkit"]);
}

- (void)testCoreDataConflicts
{
    int max = 10;
    NSError *err = nil;

    /**
     *  Make an original set
     */
    NSManagedObjectContext *moc = [self createNumbersAndSave:max];

    // there is actually `max` docs plus the metadata document
    int docs = max + 1;

    /**
     *  Push it to the primary remote DB
     */
    NSInteger count = [self pushMe];
    XCTAssertTrue(count == docs, @"push: unexpected processed objects: %@ != %d", @(count), docs);

    /**
     *  Push it to a secondary remote DB, we will treat this as an "original" copy
     */
    NSURL *originalURL = [self createSecondaryDatabase:@"-conflict"];
    count = [self pushToURL:originalURL];
    XCTAssertTrue(count == docs, @"push: unexpected processed objects: %@ != %d", @(count), docs);

    /**
     *  Kill the local DB
     */
    [self removeLocalDatabase];

    /**
     *  New Local DB
     */
    moc = self.managedObjectContext;
    XCTAssertNotNil(moc, @"could not create Context");

    /**
     *  Pull the original content
     */
    count = [self pullFromURL:originalURL];
    XCTAssertTrue(count == docs, @"push to original: unexpected processed objects: %@ != %d", @(count), docs);

    /**
     * Check for conflicts
     */
    err = nil;
    count = [self doConflictWithErr:&err];
    XCTAssertNil(err, @"processConflicts failed with error: %@", err);
    XCTAssertTrue(count == 1, @"Unexpected number of conflicts: %@ != %d", @(count), 1);

    NSArray *results;
    NSFetchRequest *fr = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    fr.shouldRefreshRefetchedObjects = YES;

    /**
     *  fetch original content
     */
    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);
    count = [results count];
    XCTAssertTrue(count == max, @"fetch: unexpected processed objects: %@ != %d", @(count), max);

    /**
     *  Modify the content by updating the creation date as well as decorate the number with 1 * max
     *  This means that we can eyeball "0d" vs "1d" vs "2d"
     */
    for (Entry *e in results) {
        long long val = [e.number longLongValue];

        e.number = @(val + (1 * max));
        e.created = [NSDate dateWithTimeIntervalSinceNow:0];
    }
    XCTAssertTrue([moc save:&err], @"MOC save failed");
    XCTAssertNil(err, @"MOC save failed with error: %@", err);

    /**
     *  Push updated data to primary DB
     */
    count = [self pushMe];
    XCTAssertTrue(count == docs, @"push to primary: unexpected processed objects: %@ != %d",
                  @(count), docs);

    /**
     *  kill local
     */
    [self removeLocalDatabase];

    /**
     *  New local
     */
    moc = self.managedObjectContext;
    XCTAssertNotNil(moc, @"could not create Context");

    /**
     *  Pull in from the original Database
     */
    count = [self pullFromURL:originalURL];
    XCTAssertTrue(count == docs, @"pull from original: unexpected processed objects: %@ != %d",
                  @(count), docs);

    /**
     * Check for conflicts
     */
    err = nil;
    count = [self doConflictWithErr:&err];
    XCTAssertNil(err, @"processConflicts failed with error: %@", err);
    XCTAssertTrue(count == 1, @"Unexpected number of conflicts: %@ != %d", @(count), 1);

    fr = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    fr.shouldRefreshRefetchedObjects = YES;

    /**
     *  fetch original context
     */
    results = [moc executeFetchRequest:fr error:&err];
    XCTAssertNotNil(results, @"Expected results: %@", err);
    count = [results count];
    XCTAssertTrue(count == max, @"fetch: unexpected processed objects: %@ != %d", @(count), max);

    /**
     *  modify again, this time value should be "2d"
     */
    for (Entry *e in results) {
        long long val = [e.number longLongValue];

        e.number = @(val + (2 * max));
        e.created = [NSDate dateWithTimeIntervalSinceNow:0];
    }
    XCTAssertTrue([moc save:&err], @"MOC save failed");
    XCTAssertNil(err, @"MOC save failed with error: %@", err);

    /**
     *  pull in the primary that is full of "1d" into our "2d" data
     */
    count = [self pullMe];
    XCTAssertTrue(count == docs, @"pull from primary: unexpected processed objects: %@ != %d",
                  @(count), docs);
    /**
     *  Check for the right number of conflicts
     */
    err = nil;
    count = [self doConflictWithErr:&err];
    XCTAssertNil(err, @"processConflicts failed with error: %@", err);
    XCTAssertTrue(count == max, @"Unexpected number of conflicts: %@ != %d", @(count), max);
}

@end
