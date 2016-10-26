#ifndef SORTED_H
#define	SORTED_H
#include "GenericDBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Pipe.h"
#include "BigQ.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <fstream>
#include <stdlib.h>
#include <string.h>
#include <math.h>

class Sorted : public GenericDBFile {
private:
    BigQ *bigQ;
    OrderMaker *sortorder;
    OrderMaker *queryOrderMaker;
    OrderMaker *cnfOrderMaker;
    int runLength;
    Pipe *inputPipe;
    Pipe *outputPipe;
    char *filename;
    bool isBinSrchReq;
    static void *GetPage(void *arg);
    static void *WritePage(void *arg);
    static void *ProcessData(void *arg);
    void CreateSortOrders(CNF &cnf);
    void initialiseBigQ();
    void BinarySearchRecords(Record &fetchMe, OrderMaker *o, Record &literal, OrderMaker *sortorder);
public:
    Sorted();
    virtual ~Sorted();
    int Create(char *f_path, fType f_type, void *startup);
    int Open(char *fpath);
    int Close();
    void Add(Record &addMe);
    void Load(Schema &mySchema, char *loadMe);
    void MoveFirst();
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchMe, CNF &applyMe, Record &literal);
    void WriteMetaData();
   
};

struct SortInfo {
    OrderMaker *myOrder;
    int runLength;
};

struct FileInfo {
    char *filename;
    Pipe *pipe;
};

struct ProcessPipeInfo {
    Pipe *sortedpipe;
    Pipe *bigqpipe;
    Pipe *outputpipe;
    OrderMaker *sortorder;
};
#endif	/* SORTED_H */

