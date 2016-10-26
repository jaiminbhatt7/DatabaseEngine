#include "BigQ.h"

/*Merge sort merges each of the sorted runs and outputs the records to the output pipe*/

void BigQ::mergeSort(int runInfo[], int numRuns) {
    File f;
    Page *p = new (std::nothrow) Page[numRuns];
    typedef priority_queue<RecordComparison*, vector<RecordComparison*>, RecordComparison> RecordPQ;
    Record r;
    f.Open(1, fileName);
    //Declare the priority queue
    RecordPQ recordPQ((RecordComparison(sortorder)));
    // initialise
    int *temprunInfo = new (std::nothrow) int[numRuns];
    /*initialise the priority queue with the first record from each run */
    for (int i = 0; i < numRuns; i++) {
        temprunInfo[i] = runInfo[i];
        f.GetPage(&p[i], temprunInfo[i]);
        p[i].GetFirst(&r);
        RecordComparison *temp = new RecordComparison;
        (temp->record).Consume(&r);
        temp->PageNumber = i;
        recordPQ.push(temp);
        temprunInfo[i]++;
    }

    RecordComparison *poprec;
    int removeindex = 0;
    /*Start the popping from the Priority Queue*/
    while (!recordPQ.empty()) {
        poprec = recordPQ.top();
        recordPQ.pop();
        removeindex = poprec->PageNumber; //get the run number from where the record was popped.
        outputPipe->Insert(&(poprec->record));
        delete poprec;
        //insert the record to the output pipe
        /*If there are more records from that run.. insert into Priority Queue*/
        if (p[removeindex].GetFirst(&r)) {
            RecordComparison *poprec = new RecordComparison;
            (poprec->record).Consume(&r);
            poprec->PageNumber = removeindex;
            recordPQ.push(poprec);
        }            //if page is empty, if there are more pages ,we need to get the next page for the run.
        else if (temprunInfo[removeindex] < runInfo[removeindex + 1]) {
            p[removeindex].EmptyItOut();
            f.GetPage(&p[removeindex], temprunInfo[removeindex]);
            temprunInfo[removeindex]++;
            p[removeindex].GetFirst(&r);
            RecordComparison *poprec = new RecordComparison;
            (poprec->record).Consume(&r);
            poprec->PageNumber = removeindex;
            recordPQ.push(poprec);
        }
    } //end outer while
    f.Close();
    delete [] p;
    delete []temprunInfo;
} //end function

int BigQ::generateRun(vector<Record*> &recordArray, int lastPageCount) {
    int k;
    Page p;
    File f;
    //sort the records in the run
    //write the sorted run in the file
    f.Open(1, fileName);
    /*Once sorted, append the records from record array back to the page and keep a count of pages as page count may increase
      after appending the sorted records.*/
    k = 0;
    int remPages = 1;
    int numRec = recordArray.size();
    /*If append fails, add the current page to the file at correct position*/
    for (int j = 0; j < numRec; j++) {
        if (!p.Append(recordArray[j])) {
            remPages = 0;
            f.AddPage(&p, (lastPageCount) + k);
            k++;
            p.EmptyItOut();
            p.Append(recordArray[j]);
            remPages = 1;
        }
    }
    //add the last page to the file
    if (remPages == 1) {
        f.AddPage(&p, (lastPageCount) + k);
        p.EmptyItOut();
    }
    recordArray.clear();
    f.Close();
    return (lastPageCount + k + 1);
}

/*This function reads records from the input pipe. When we read run length pages of records, we 
call generate run method that generates
a sorted run of the unsorted run*/
void *BigQ::start(void  *arg) {
    static int filenameCount = 0;
    BigQ *bigqutil = (BigQ *) arg;
    File f;
    Page p;
    Record r, *temprec, temprec2;
    vector<Record *> recordArray; //this vector holds the records every run
    vector <int> runInfotemp; //this vector holds the start page count of every run
    int pageCount = 0, lastPageCount = 0;
    int numRuns = 0;
    sprintf(bigqutil->fileName,"%d.bigqtmp",filenameCount++);
    //cout<< bigqutil->fileName;
    f.Open(0, bigqutil->fileName);
    f.Close();
    /*Continue reading from the input pipe till input pipe is not empty*/
    Compare compare(bigqutil->sortorder);
    while (bigqutil->inputPipe->Remove(&r)) {
        if (!p.Append(&r)) {
            //Append each record in the page
            temprec2.Consume(&r);
            while (p.GetFirst(&r)) {
                temprec = new Record;
                temprec->Consume(&r);
                recordArray.push_back(temprec);
            }
            pageCount++;
            //Add currPage to the file , then increment page count
            if ((pageCount - lastPageCount) == bigqutil->runLength) {
                //call generate run that gives a sorted run and returns start pagecount of that run
                numRuns++;
                sort(recordArray.begin(), recordArray.end(), compare);
                pageCount = bigqutil->generateRun(recordArray, lastPageCount);
                lastPageCount = pageCount;
                runInfotemp.push_back(pageCount);
            }
            p.EmptyItOut();
            p.Append(&temprec2);
        }
    }
    while (p.GetFirst(&r)) {
        temprec = new Record;
        temprec->Consume(&r);
        recordArray.push_back(temprec);
    }
    p.EmptyItOut();
    if (recordArray.size() > 0) {
        numRuns++;
        sort(recordArray.begin(), recordArray.end(), compare);
        if (numRuns > 1) {
            pageCount = bigqutil->generateRun(recordArray, lastPageCount);
            runInfotemp.push_back(pageCount);
        }
    }
    if (numRuns > 1) {
        int runInfo[numRuns + 1];
        vector<int>::iterator it;
        int k = 0;
        runInfo[k++] = 0;
        for (it = runInfotemp.begin(); it < runInfotemp.end(); it++) {
            runInfo[k++] = *it;
        }
        bigqutil->mergeSort(runInfo, numRuns);
    } else if (numRuns == 1) {
        int numRec = recordArray.size();
        /*If append fails, add the current page to the file at correct position*/
        //cout << "Here" << endl;
        for (int j = 0; j < numRec; j++) {
            bigqutil->outputPipe->Insert(recordArray[j]);
        }
        recordArray.clear();
    }
    remove(bigqutil->fileName);
    bigqutil->outputPipe->ShutDown();
    //delete bigqutil->outputPipe;
}

/*BigQ constructor*/
BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    this->inputPipe = &in;
    this->outputPipe = &out;
    this->sortorder = &sortorder;
    this->runLength = runlen;
    pthread_t sortThread;
    //join the threads
    pthread_create(&sortThread, NULL, BigQ::start, this);
    // finally shut down the out pipe
}

BigQ::~BigQ() {
}