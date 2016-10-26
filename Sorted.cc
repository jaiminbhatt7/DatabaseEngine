#include "Sorted.h"

Sorted::Sorted() {
    this->remPages =1;
    this->currentPageCount=0;
    this->queryOrderMaker = NULL;
    this->isBinSrchReq = true;
}

Sorted::~Sorted() {
    delete this->inputPipe;
    delete this->outputPipe;
    delete this->bigQ;
}
//Open - /*In this function we open the metadata file and reconstruct the ordermaker*/
int Sorted::Open(char *f_path) {

  string fileType;
  string line;
  string fileName=(string)f_path+".metadata";
  string str_numAtts;
  const char * token;
  char * ch;
  int numAtts;
  ifstream fin (fileName.c_str());
  if (fin.is_open())
  {
     getline(fin,fileType);
     getline (fin,str_numAtts);
     token=(const char *)str_numAtts.c_str();
     ch=strtok((char *)token,"= ");
     ch=strtok(NULL,"= ");
     numAtts=atoi(ch);
     if(numAtts > 0 )
     {
      sortorder=new OrderMaker;
      sortorder->numAtts=numAtts;

     for (int i = 0; i < numAtts; i++)
     {
                getline (fin,line);
                token=(const char *)line.c_str();
                ch=strtok((char *)token,": ");
                ch=strtok(NULL,": ");
                sortorder->whichAtts[i]=atoi(ch);
                getline (fin,line);
                token=(const char *)line.c_str();
                if (strcmp(token,"Int")==0)
                {
                   sortorder->whichTypes[i]=Int;
                }//int
                else if (strcmp(token, "Double") == 0) {
                   sortorder->whichTypes[i] = Double;
                }//double
                else {
                    sortorder->whichTypes[i] = String;
                }
            }
        }
    }
    //this->sortorder->Print();
    getline(fin, line);
    token = (const char *) line.c_str();
    this->runLength = atoi(token);
    this->filename = f_path;
    initialiseBigQ();
    outFile.Open(1, f_path);
    return 1;
}

/*In this function we create the meta data file and write out the text version of the ordermaker*/
int Sorted::Create(char *f_path, fType f_type, void *startup) {
    SortInfo *start = (SortInfo *) startup;
    string fileName = (string) f_path + ".metadata";
    ofstream fout(fileName.c_str());
    fout << "sorted" << endl;
    fout << "NumAtts=" << start->myOrder->numAtts << endl;
    for (int i = 0; i < start->myOrder->numAtts; i++) {
        fout << i << ":" << start->myOrder->whichAtts[i] << endl;
        if (start->myOrder->whichTypes[i] == Int)
            fout << "Int" << endl;
        else if (start->myOrder->whichTypes[i] == Double)
            fout << "Double" << endl;
        else
            fout << "String" << endl;
    }
    fout << start->runLength << endl;
    fout.close();
    this->runLength = start->runLength;
    this->sortorder = start->myOrder;
    this->filename = f_path;
    initialiseBigQ();
    outFile.Open(0, f_path);
    return 1;
}

/*Initialise the input and output pipe*/
void Sorted::initialiseBigQ() {
    this->inputPipe = new Pipe(100);
    this->outputPipe = new Pipe(100);
    this->bigQ = new BigQ(*(this->inputPipe), *(this->outputPipe), *this->sortorder, runLength);
}

void Sorted::MoveFirst() {
    GenericDBFile::MoveFirst();
}

int Sorted::Close() {
    this->inputPipe->ShutDown();
    return (GenericDBFile::Close());
}

/*This getnext calls genericDBFile's getnext method*/
int Sorted::GetNext(Record &fetchme) {
    return (GenericDBFile::GetNext(fetchme));
}

void Sorted::Add(Record &rec) {
    inputPipe->Insert(&rec);
}

void Sorted::Load(Schema &f_schema, char *loadpath) {
    FILE * tbl = fopen(loadpath, "r"); // open The .tbl file
    this->currentPageCount = 0; //set The current page count =0
    Record rec;
    //get next record from The table into r
    while (rec.SuckNextRecord(&f_schema, tbl)) {
        //call add function to add records
        inputPipe->Insert(&rec);
    }
    fclose(tbl);
}

void *Sorted::GetPage(void* arg) {
    FileInfo *fileinfo1 = (FileInfo *) arg;
    File infile;
    infile.Open(1, fileinfo1->filename);
    Page p;
    Record r;
    for (int i = 0; i < infile.GetLength() - 1; i++) {
        infile.GetPage(&p, i);
        while (p.GetFirst(&r)) {
            fileinfo1->pipe->Insert(&r);
        }
    }
    infile.Close();
    fileinfo1->pipe->ShutDown();
}

void *Sorted::WritePage(void* arg) {
    FileInfo *fileinfo2 = (FileInfo *) arg;
    File outfile;
    outfile.Open(0, fileinfo2->filename);
    Page p;
    Record r;
    int i = 0;
    int remPages = 1;
    while (fileinfo2->pipe->Remove(&r)) {
        if (!p.Append(&r)) {
            remPages = 0;
            outfile.AddPage(&p, i++);
            p.EmptyItOut();
            p.Append(&r);
            remPages = 1;
        }
    }
    if (remPages == 1) {
        outfile.AddPage(&p, i);
        p.EmptyItOut();
    }
    outfile.Close();
}

void *Sorted::ProcessData(void* arg) {
    ComparisonEngine ceng;
    ProcessPipeInfo *processInfo = (ProcessPipeInfo *) arg;
    Record outputPiperec;
    Record mergedSortedrec;
    int outputPipeEmpty, sortedFileEmpty;
    outputPipeEmpty = processInfo->bigqpipe->Remove(&outputPiperec);
    sortedFileEmpty = processInfo->sortedpipe->Remove(&mergedSortedrec);
    while (outputPipeEmpty == 1 && sortedFileEmpty == 1) {
        if (ceng.Compare(&mergedSortedrec, &outputPiperec, processInfo->sortorder) < 0) {
            processInfo->outputpipe->Insert(&mergedSortedrec);
            sortedFileEmpty = processInfo->sortedpipe->Remove(&mergedSortedrec);
        } else {
            processInfo->outputpipe->Insert(&outputPiperec);
            outputPipeEmpty = processInfo->bigqpipe->Remove(&outputPiperec);
        }
    }
    if (outputPipeEmpty == 1 && sortedFileEmpty == 0) {
        processInfo->outputpipe->Insert(&outputPiperec);
        while (processInfo->bigqpipe->Remove(&outputPiperec)) {
            processInfo->outputpipe->Insert(&outputPiperec);
        }
    } else if (outputPipeEmpty == 0 && sortedFileEmpty == 1) {
        processInfo->outputpipe->Insert(&mergedSortedrec);
        while (processInfo->sortedpipe->Remove(&mergedSortedrec)) {
            processInfo->outputpipe->Insert(&mergedSortedrec);
        }
    }
    processInfo->outputpipe->ShutDown();
}

void Sorted::WriteMetaData() {
    //Merge the two files together
    this->inputPipe->ShutDown();
    if ((this->outFile.GetLength()) == 0) {
        Record outputPiperec;
        while (this->outputPipe->Remove(&outputPiperec)) {
            GenericDBFile::Add(outputPiperec);
        }
        if (this->remPages = 1) {
            this->outFile.AddPage(&this->currentPage, this->currentPageCount++); //Add The current page count to The file , then increment The current page count
            this->currentPage.EmptyItOut();
            this->remPages = 0;
        }
    } else {
        this->outFile.Close();
        //Start the Merge
        Pipe input(100);
        Pipe output(100);
        FileInfo fileinfo1 = {this->filename, &input};
        FileInfo fileinfo2 = {"heap.bin", &output};
        ProcessPipeInfo processInfo = {&input, this->outputPipe, &output, (this->sortorder)};
        pthread_t readThread, writeThread, processThread;
        pthread_create(&readThread, NULL, &Sorted::GetPage, (void *) &fileinfo1);
        pthread_create(&writeThread, NULL, &Sorted::WritePage, (void *) &fileinfo2);
        pthread_create(&processThread, NULL, &Sorted::ProcessData, (void *) &processInfo);
        pthread_join(readThread, NULL);
        pthread_join(writeThread, NULL);
        pthread_join(processThread, NULL);
        remove(this->filename);
        rename("heap.bin", this->filename);
        this->outFile.Open(1, this->filename);
    }
    delete this->inputPipe;
    delete this->outputPipe;
    delete this->bigQ;
    initialiseBigQ();
}

void Sorted::CreateSortOrders(CNF &cnf) {
    OrderMaker copySortOrder;
    copySortOrder = *sortorder;
    cnfOrderMaker = new OrderMaker;
    queryOrderMaker = new OrderMaker;
    cnf.GetSortOrders2(*cnfOrderMaker, copySortOrder);
    int i, j;
    for (i = 0; i < sortorder->numAtts; i++) {
        for (j = 0; j < cnfOrderMaker->numAtts; j++) {
            if (sortorder->whichAtts[i] == cnfOrderMaker->whichAtts[j]) {
                queryOrderMaker->whichTypes[i] = sortorder->whichTypes[i];
                queryOrderMaker->whichAtts[i] = j;
                queryOrderMaker->numAtts++;
                break;
            }
        }
        if (j == cnfOrderMaker->numAtts)
            break;

    }
    //cnfOrderMaker->Print();
    //queryOrderMaker->Print();
}

/*Perform Binnay search for the page that contains the record*/
void Sorted::BinarySearchRecords(Record &fetchMe, OrderMaker* o, Record &literal, OrderMaker* sortorder) {
    
    ComparisonEngine comp;
    int compareRes, start, end, fileMidLength;
    bool found = 0;
    bool once = 0;
    start = 0;
    end = outFile.GetLength() - 2;
    while (start < end && found == 0) {
        fileMidLength = start + floor((float) (end - start) / 2);
        outFile.GetPage(&(this->currentPage), fileMidLength);
        while (this->currentPage.GetFirst(&fetchMe)) // get first record
        {
            compareRes = comp.Compare(&literal, this->queryOrderMaker, &fetchMe, this->sortorder);
            if (compareRes == 0) {
                found = 1;
                fileMidLength--;
                if (fileMidLength == 0) break;
                outFile.GetPage(&(this->currentPage), fileMidLength);
            } else if (compareRes < 0) {
                if (found) break;
                end = fileMidLength - 1;
                break;
            } else if (compareRes > 0) {
                if (found) break;
                start = fileMidLength;
                if (end - start == 1) once = 1;
                if (once) {
                    start = fileMidLength + 1;
                }
                break;
            }
        }
    }
    if (found == 1) {
        this->currentPageCount = fileMidLength;
        outFile.GetPage(&(this->currentPage), fileMidLength);
    }

    if (start == end && found == 0) {
        this->currentPageCount = start;
        outFile.GetPage(&(this->currentPage), start);
    }
}

int Sorted::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    
    ComparisonEngine ceng;
    if (this->isBinSrchReq) {
        this->isBinSrchReq = false;
        if (this->queryOrderMaker == NULL) {
            this->CreateSortOrders(cnf);
            this->isBinSrchReq = false;
        }
        if (this->queryOrderMaker->numAtts > 0) {
            BinarySearchRecords(fetchme, (this->queryOrderMaker), literal, sortorder);
        }
    }
    if (this->queryOrderMaker->numAtts == 0) {
        return (GenericDBFile::GetNext(fetchme, cnf, literal));
    } else {
        while (GetNext(fetchme)) {
            if (ceng.Compare(&literal, this->queryOrderMaker, &fetchme, this->sortorder) >= 0) {
                //cerr<<"\nComparing literal record with fetched record";
                if (ceng.Compare(&fetchme, &literal, &cnf)) {
                    return 1;
                }
            } else {
                return 0;
            }
        }
        return 0;
    }
}

