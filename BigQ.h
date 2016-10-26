#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <algorithm>
#include <vector>
#include <stdio.h>
#include <queue>
#include <string.h>

using namespace std;
//*********************

static int filenameCount = 0;

class RecordComparison {
public:
    OrderMaker *order;
    Record record;
    int PageNumber;

    RecordComparison() {
    }

    RecordComparison(OrderMaker *ord) {
        order = ord;
    }

    bool operator ()(RecordComparison *left, RecordComparison *right) const {
        ComparisonEngine comp;
        if (comp.Compare(&(left->record), &(right->record), order) >= 0) // compare returns 0 is left is less than right
            return true;
        else
            return false;
    }
};

class Compare {
private:
    OrderMaker *sortorder;
public:

    Compare(OrderMaker *sortorder) {
        this->sortorder = sortorder;
    }

    bool operator() (Record *left, Record *right) const {
        ComparisonEngine comp;
        if (comp.Compare(left, right, (sortorder)) < 0) {
            return true;
        } else {
            return false;
        }
    }
};

class BigQ {
private:
    Pipe *inputPipe;
    Pipe *outputPipe;
    OrderMaker *sortorder;
    int runLength;
    char fileName[10];
    static void *start(void * arg);
    int generateRun(vector<Record*> &recordArray, int lastPageCount);
    void mergeSort(int runInfo[], int numRuns);
public:
    BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    ~BigQ();
};




#endif