#include "RelOp.h"


/*void CompareleftAndVector(vector<Record*> V);

void CompareleftAndVector(vector<Record*> V) {
	cout << "Right pipe exhausted " << endl;
	cout << "Comparing left and vector" << endl;
}*/


// ------------ SELECT PIPE --------------------------
/*
 * SelectPipe takes two pipes as input: an input pipe and an output pipe. It also takes a
CNF. It simply applies that CNF to every tuple that comes through the pipe, and every
tuple that is accepted is stuffed into the output pipe.
 *
 */
void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{
	pthread_create(&m_thread, NULL, executeThread,
			(void*)new Relation(&inPipe, &outPipe, &selOp, &literal));
}

void* SelectPipe::executeThread(void* rel)
{
	//cout<<"Select pipe executethread"<<endl;
	Relation* input = (Relation*)rel;
	Record rec ;
	ComparisonEngine compare;
	while(input->iPipe->Remove(&rec))
	{
		if(compare.Compare(&rec,input->litRec,input->cnf)==1)	//cnf accepts the record put in output pipe
		{
			input->oPipe->Insert(&rec);

		}


	}
	input->oPipe->ShutDown();
	delete input;
	input=NULL;

}

// ------------ SELECT FILE --------------------------
/*
 * SelectFile takes a DBFile and a pipe as input. You can assume that this file is all
set up; it has been opened and is ready to go. It also takes a CNF. It then performs a scan
of the underlying file, and for every tuple accepted by the CNF, it stuffs the tuple into the
pipe as output. The DBFile should not be closed by the SelectFile class; that is the
job of the caller.
 *
 */
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{
	pthread_create(&m_thread, NULL, executeThread,
			(void*)new Relation( &inFile, &outPipe,  &selOp, &literal));
}

void* SelectFile::executeThread(void* rel)
{
	//cout<<"Selectfile executethread"<<endl;
	Relation* input = (Relation*)rel;
	Record rec ;
	ComparisonEngine compare;

	input->cnf->Print();
	int k=0;
	//while(input->iFile->GetNext(*rec))
	while (input->iFile->GetNext(rec,*(input->cnf),*(input->litRec)))
	{
		//cout<<k++<<endl;	//cnf accepts the record put in output pipe
		input->oPipe->Insert(&rec);

	}
	//cout<<"Done"<<endl;
	input->oPipe->ShutDown();
	delete input;
	input = NULL;
	//pthread_exit(0);
}

// ------------ PROJECT --------------------------
/*
 * Project takes an input pipe and an output pipe as input. It also takes an array of
integers keepMe as well as the number of attributes for the records coming through the
input pipe and the number of attributes to keep from those input records. The array of
integers tells Project which attributes to keep from the input records, and which order
to put them in. So, for example, say that the array keepMe had the values [3, 5, 7,
1]. This means that Project should take the third attribute from every input record and
treat it as the first attribute of those records that it puts into the output pipe. Project
should take the fifth attribute from every input record and treat it as the second attribute
of every record that it puts into the output pipe. The seventh input attribute becomes the
third. And so on.
 *
 */
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe,
		int numAttsInput, int numAttsOutput)
{
	// Create thread to do the project operation
	pthread_create(&m_thread, NULL, executeThread,
			(void*) new Relation(&inPipe, &outPipe, keepMe, numAttsInput, numAttsOutput));

	return;
}



void* Project::executeThread(void* rel)
{
	Relation* input = (Relation*)rel;
	Record *rec = new Record();

	while(input->iPipe->Remove(rec))
	{

		rec->Project(input->pAttsNew, input->numAttsNew, input->numAttsOrig);
		input->oPipe->Insert(rec);
		rec = new Record();
	}
	input->oPipe->ShutDown();
	//pthread_exit(0);
}

// ------------ SUM------------------------
/*Sum computes the SUM SQL aggregate function over the input pipe, and puts a single
tuple into the output pipe that has the sum. The function over each tuple (for example:
(l_extendedprice*(1-l_discount)) in the case of the TPC-H schema) that is
summed is stored in an instance of the Function class that is also passed to Sum as an
argument

 */
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe)
{
	// Create thread to do the project operation
	pthread_create(&m_thread, NULL, executeThread,
			(void*) new Relation(&inPipe, &outPipe, &computeMe));

	return;
}

void * Sum::executeThread(void * rel)
{
	Relation* input = (Relation*)rel;
	Record rec;
	double sum=0;
	int value=0;
	double returnval=0;
	//peform sum
	while(true)
	{
		value =0;
		returnval=0;
		if(input->iPipe->Remove(&rec))
		{
			input->Func->Apply(rec, value, returnval);
			sum += returnval + value;
		}
		else
			break;
	}
	Attribute sumatt = {(char*)"sum", Double};

	Schema sum_schema((char*)"tmp_sum_schema_file", 1, &sumatt) ;// (filename, not used,number of attributes, attribute pointer)

	FILE * tmp_file = fopen("tmp_file_sum_relop", "w");
	fprintf(tmp_file, "%f|", sum);
	fclose(tmp_file);
	tmp_file = fopen("tmp_file_sum_relop", "r");
	rec.SuckNextRecord(&sum_schema, tmp_file);
	fclose(tmp_file);

	input->oPipe->Insert(&rec);
	input->oPipe->ShutDown();
	delete input;
	input =NULL;
	// delete file tmp_file_sum_relop
	if(remove("tmp_file_sum_relop") != 0)
		perror("\nError in removing tmp_sum_data_file!");
	pthread_exit(0);

}

//---Writeout----------

/*
 * WriteOut accepts an input pipe, a schema, and a FILE*, and uses the schema to write
text version of the output records to the file.
 *
 */

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema)
{	pthread_create(&m_thread, NULL, &executeThread,
		(void*) new Relation(&inPipe, &mySchema, outFile));

return;
}

void * WriteOut::executeThread(void * rel)
{
	Relation* input = (Relation*)rel;
	Record rec;
	int currcount = 0;

	while(input->iPipe->Remove(&rec))
	{
		currcount++;
		int total_atts = input->iSchema->GetNumAtts();       //get the info of the records ..attributes etc
		Attribute *atts = input->iSchema->GetAtts();

		for (int i = 0; i < total_atts; i++)  //for all the attributes
		{
			// print information of each attribute to the output file
			fprintf(input->ifile, "%s: ", atts[i].name);
			int offsetposition = ((int *) rec.bits)[i + 1]; // get offset to attribute in record
			// depending on the type we then print out the contents , ie . int float or string
			fprintf(input->ifile, "[");

			if (atts[i].myType == Int)
			{
				int *myInt = (int *) &(rec.bits[offsetposition]);
				fprintf(input->ifile, "%d", *myInt);

			}
			else if (atts[i].myType == Double)
			{
				double *myDouble = (double *) &(rec.bits[offsetposition]);
				fprintf(input->ifile, "%f", *myDouble);

			}
			else if (atts[i].myType == String)
			{
				char *myString = (char *) &(rec.bits[offsetposition]);
				fprintf(input->ifile, "%s", myString);
			}
			fprintf(input->ifile, "]");


			if (i != total_atts - 1)
			{
				fprintf(input->ifile, ", "); //between data
			}
		}
		fprintf(input->ifile, "\n");
	}

	input->count = &currcount;
	input = NULL;
	//pthread_exit(0);
}


//-------Duplicateremovel
/*
 * DuplicateRemoval takes an input pipe, an output pipe, as well as the schema for the
tuples coming through the input pipe, and does a duplicate removal. That is, everything
that comes through the output pipe will be distinct. It will use the BigQ class to do the
duplicate removal. The OrderMaker that will be used by the BigQ (which you’ll need
to write some code to create) will simply list all of the attributes from the input tuples.
 *
 */
void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
	i_runlen = 10;

	if (i_runlen < 0)
	{
		cerr << "\nError! Incorrect runlength \n";
		exit(1);
	}

	pthread_create(&m_thread, NULL, &executeThread,
			(void*) new Relation(&inPipe, &outPipe, &mySchema, i_runlen));
	return;
}

void * DuplicateRemoval::executeThread(void * rel)
{
	Relation* input = (Relation*)rel;
	bool fl_lastrec = false;
	Record lastRec ;
	Record currentRec ;
	OrderMaker sortOrder;
	ComparisonEngine ce;

	int size = 100;
	Pipe resultPipe(size);
	BigQ B(*(input->iPipe), resultPipe, sortOrder,input->runlen);


	// create new ordermaker using all attributes
	sortOrder.numAtts = input->iSchema->GetNumAtts();
	Attribute *atts = input->iSchema->GetAtts();
	for (int i = 0; i < sortOrder.numAtts; i++)
	{
		sortOrder.whichAtts[i] = i;
		sortOrder.whichTypes[i] = atts[i].myType;
	}
	while (resultPipe.Remove(&currentRec))
	{
		if (fl_lastrec == false)
		{
			lastRec.Copy(&currentRec);
			fl_lastrec = true;
		}

		// compare records .. if same, its duplicate dont pu tin result
		// if different, send last seem rec to outputPipe

		if (ce.Compare(&lastRec, &currentRec, &sortOrder) != 0)
		{
			input->oPipe->Insert(&lastRec);
			lastRec.Copy(&currentRec);
		}

	}

	// last record has not been inserted
	input->oPipe->Insert(&lastRec);
	resultPipe.ShutDown();
	input->oPipe->ShutDown();

	input = NULL;
}

//----------wait until procedures -- join n the thread --block until complete

void SelectFile::WaitUntilDone () {
	pthread_join (m_thread, NULL);
}

void SelectPipe::WaitUntilDone () {
	pthread_join (m_thread, NULL);
}

void Project::WaitUntilDone()
{
	pthread_join(m_thread, NULL);
}

void Sum::WaitUntilDone()
{
	pthread_join(m_thread, NULL);
}

void DuplicateRemoval::WaitUntilDone()
{
	pthread_join(m_thread, NULL);
}
/*
void GroupBy::WaitUntilDone()
{
	cout << " groupby wait bef " <<endl;
	pthread_join(m_thread, NULL);
	cout << " groupby wait aft " <<endl;
}*/
void WriteOut::WaitUntilDone()
{
	pthread_join(m_thread, NULL);
}


//--------usenPages for all relational operators
void DuplicateRemoval::Use_n_Pages(int runlen){
	//cout<<"Duplicate removal use_n_pages called"<<endl;
	i_runlen = runlen;
}

void SelectFile::Use_n_Pages (int runlen)
{
	//cout<<"SelectFile use_n_pages called"<<endl;
	runlength = runlen;
}

void Sum::Use_n_Pages (int runlen)
{
	//cout<<"Sum use_n_pages called"<<endl;
}
void Project::Use_n_Pages (int runlen)
{
	//cout<<"Project use_n_pages called"<<endl;
}
void SelectPipe::Use_n_Pages (int runlen)
{
	//cout<<"SelectPipe use_n_pages called"<<endl;
}/*
void GroupBy::Use_n_Pages (int runlen)
{
	cout<<"Groupby use_n_pages called"<<endl;
	runLen = runlen;
}*/
void WriteOut::Use_n_Pages (int runlen)
{
	//cout<<"Writeout use_n_pages called"<<endl;
}


void* GBThread(void* arg)
		{
	GroupBy* input = (GroupBy*)arg;

	Pipe sortedOutPipe(input->pipe_size);

	BigQ(*(input->inPipe), sortedOutPipe, *(input->groupAtts), input->runlen);

	Record tempRec;
	Record nextRec;
	Record temp;
	ComparisonEngine comp;
	int l = sortedOutPipe.Remove(&tempRec);
	while(l)
	{

		//cout<<"GB1\n";
		temp.Copy(&tempRec);
		Sum S;
		Pipe sumPipeIn(input->pipe_size);
		Pipe sumPipeOut(input->pipe_size);
		sumPipeIn.Insert(&temp);

		S.Run(sumPipeIn, sumPipeOut, *(input->computeMe));

		while((l = sortedOutPipe.Remove(&temp)) && !comp.Compare(&tempRec,&temp, input->groupAtts))
		{
			sumPipeIn.Insert(&temp);
		}
		//cout<<"GB2\n";
		sumPipeIn.ShutDown();
		S.WaitUntilDone();
		//cout<<"GB3\n";
		Record sumResult;
		sumPipeOut.Remove(&sumResult);
		Record GBMerge;

		GBMerge.MergeGBRecords(&sumResult, &tempRec, input->groupAtts);
		//sumPipeOut.ShutDown();
		input->outPipe->Insert(&GBMerge);
		if(l)
		{
			tempRec.Copy(&temp);
		}
		//cout<<"GB4\n";
	}

	input->outPipe->ShutDown();
		}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
{
	GroupBy* input = new GroupBy(inPipe,outPipe, groupAtts, computeMe);
	pthread_create(&thread, NULL, GBThread, (void*) input);
}


Join :: Join(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal, int runlen)
{
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	this->runlen = runlen;
}



void Sort_Merge(Pipe& inPipeL, Pipe& inPipeR, Pipe& outPipe, OrderMaker& left, OrderMaker& right, Record& literal, CNF& selOp)
{
	Record recL, recR;
	ComparisonEngine comp;
	Record tempRec;

	int l = inPipeL.Remove(&recL);
	int r = inPipeR.Remove(&recR);

	int count = 0;
	//cout<<"Printing CNF\n";
	//selOp.Print();


	while(l && r)
	{

		int min = comp.Compare(&recL, &left, &recR, &right);
		switch(min)
		{
		case -1:

		while((l=inPipeL.Remove(&tempRec)) && !comp.Compare(&recL, &tempRec, &left));

		if(l)
		{
			recL.Consume(&tempRec);
		}

		break;

		case 1:
			while((r=inPipeR.Remove(&tempRec)) && !comp.Compare(&recR, &tempRec, &right));

			if(r)
			{
				recR.Consume(&tempRec);
			}

			break;

		case 0:
			vector<Record*> leftArr;
			vector<Record*> rightArr;

			leftArr.push_back(&recL);
			rightArr.push_back(&recR);
			Record* temp;
			Record* nextRecL = new Record;

			while((l=inPipeL.Remove(nextRecL)) && !comp.Compare(&recL, nextRecL, &left))
			{
				temp = new Record;
				temp->Consume(nextRecL);
				leftArr.push_back(temp);
			}


			Record* nextRecR = new Record;
			while((r=inPipeR.Remove(nextRecR)) && !comp.Compare(&recR, nextRecR, &right))
			{
				temp = new Record;
				temp->Consume(nextRecR);
				rightArr.push_back(temp);

			}



			int numAttsLeft = ((int *) recL.GetBits())[1] / sizeof(int) - 1;
			int numAttsRight = ((int *) recR.GetBits())[1] / sizeof(int) - 1;
			int numAttsToKeep = numAttsLeft + numAttsRight;
			int* attsToKeep = new int[numAttsToKeep];

			for(int i =0;i<numAttsLeft;i++)
				attsToKeep[i] = i;

			int startOfRight = numAttsLeft;

			for(int i=numAttsLeft; i<(numAttsRight+numAttsLeft);i++)
				attsToKeep[i] = i-numAttsLeft;


			for(int i=0;i<leftArr.size();i++)
			{
				for(int j=0;j<rightArr.size();j++)
				{


					if(comp.Compare(leftArr[i], rightArr[j], &literal, &selOp ))
					{

						Record mergedRec;

						mergedRec.MergeRecords(leftArr[i], rightArr[j], numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, startOfRight);


						count++;
						outPipe.Insert(&mergedRec);

					}
				}


			}

			/*for(int i =0;i<leftArr.size();i++)
					{
					    if(leftArr[i]!=NULL)
					    delete (Record*)leftArr[i];
					}

					for(int j=0;j<rightArr.size();j++)
					{
					    if(rightArr[j]!=NULL)
					    delete (Record*)rightArr[j];
					}*/

			leftArr.clear();
			rightArr.clear();
			if(l)
			{
				recL.Consume(nextRecL);

			}

			if(r)
			{
				recR.Consume(nextRecR);

			}
			break;


		}

	}

	//cout<<"Join records = "<<count<<endl;

}

void Block_Join(vector<Record*> leftArr, DBFile& fileR, Join* input, string fileName)
{
	vector<Record*> rightArr;

	fileR.Open((char*)fileName.c_str());
	fileR.MoveFirst();

	Record* recR = new Record;

	int maxSizeR = (input->runlen)*PAGE_SIZE;
	int curSizeR = sizeof(int)* (input->runlen);

	ComparisonEngine comp;

	int r = fileR.GetNext(*recR);

	int numAttsLeft, numAttsRight, numAttsToKeep, *attsToKeep, startOfRight;

	if(r)
	{
		rightArr.push_back(recR);
		numAttsLeft = ((int *) leftArr[0]->GetBits())[1] / sizeof(int) - 1;
		numAttsRight = ((int *) rightArr[0]->GetBits())[1] / sizeof(int) - 1;
		numAttsToKeep = numAttsLeft + numAttsRight;
		attsToKeep = new int[numAttsToKeep];

		for(int i =0;i<numAttsLeft;i++)
			attsToKeep[i] = i;

		startOfRight = numAttsLeft;

		int k=0;
		int m = numAttsRight+numAttsLeft;

		for(int i=numAttsLeft; i<m;i++)
			attsToKeep[i] = k++;


	}


	static int cnt;

	while(r)
	{
		recR = new Record;
		while((r=fileR.GetNext(*recR)) && (curSizeR += recR->getSize())<= maxSizeR)
		{
			rightArr.push_back(recR);
			recR = new Record;
		}

		for(int i=0;i<leftArr.size();i++)
		{
			for(int j=0; j<rightArr.size(); j++)
			{
				if(comp.Compare(leftArr[i], rightArr[j], input->literal, input->selOp))
				{
					Record mergeRec;
					mergeRec.MergeRecords(leftArr[i], rightArr[j], numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, startOfRight);
					input->outPipe->Insert(&mergeRec);
					cnt++;
				}
			}
		}

		rightArr.clear();
		if(r)
		{
			rightArr.push_back(recR);
			curSizeR = sizeof(int)* (input->runlen) + recR->getSize();
		}
	}

	//cout<<"Join Records: "<<cnt<<endl;

}

void* JoinThread(void* arg)
		{
	Join* input = (Join*)arg;
	Record temp;

	OrderMaker left, right;

	int x = input->selOp->GetSortOrders(left,right);

	if(x!=0)
	{
		//left.Print();
		//right.Print();
		//cout<<"After printing OMs\n";
		Pipe outPipeL(input->pipe_size);
		Pipe outPipeR(input->pipe_size);
		//cout<<"Calling sort merge\n";
		//cout<<"runlen"<<input->runlen<<endl;
		BigQ ql(*(input->inPipeL),outPipeL, left, input->runlen);
		BigQ qr(*(input->inPipeR),outPipeR,right,input->runlen);

		Sort_Merge(outPipeL,outPipeR,*(input->outPipe), left, right, *(input->literal), *(input->selOp));
		//input->outPipe->ShutDown();
	}

	else
	{
		//cout<<"Block Join\n";
		Record tempRecR;
		DBFile fileR;
		int randomN = random()%10000000;
		ostringstream o;
		o<<randomN;
		string fileName = "BlockJoinR";
		fileName.append(o.str());
		fileName.append(".bin");
		fileR.Create((char*)fileName.c_str(), heap, NULL);
		while(input->inPipeR->Remove(&tempRecR))
			fileR.Add(tempRecR);
		fileR.Close();

		vector<Record*> leftArr;
		Record* recL;
		int curSizeL = sizeof(int)* (input->runlen);
		int maxSizeL = (input->runlen) * PAGE_SIZE;

		recL = new Record;

		int l = input->inPipeL->Remove(recL);
		if(l)
			leftArr.push_back(recL);

		while(l)
		{
			recL = new Record;
			while((l=input->inPipeL->Remove(recL)) && (curSizeL += recL->getSize() ) <= maxSizeL )
			{
				leftArr.push_back(recL);
				//curSizeL += recL.getSize();
				recL = new Record;

			}

			Block_Join(leftArr, fileR, input, fileName);

			//for(int i =0; i<leftArr.size(); i++)
				leftArr.clear();
			if(l)
			{
				leftArr.push_back(recL);
				curSizeL =  sizeof(int)* (input->runlen) + recL->getSize() ;
			}


		}


		remove(fileName.c_str());
		fileName.append(".header");
		remove(fileName.c_str());

	}

	input->outPipe->ShutDown();
	delete input;


		}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {

	Join *input = new Join(inPipeL,inPipeR,outPipe,selOp,literal,runlen);
	pthread_create (&thread, NULL, JoinThread, (void *)input);
}

void Join::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void Join::Use_n_Pages (int runlen) {

	this-> runlen = runlen;

}


