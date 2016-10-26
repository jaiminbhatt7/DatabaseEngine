#ifndef RELOP_H
#define RELOP_H

#include <pthread.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <fstream>
#include <sstream>
#include <vector>
#include <time.h>

struct join_struct {
	Pipe *inPipeL;
	Pipe *inPipeR;
	Pipe *outPipe;
	CNF *cnf;
	Record *literal;
	int runlength;

};

class RelationalOp {
public:
	// blocks the caller until the particular relational operator
	// has run to completion
	int runlen;
		int pipe_size;

		// blocks the caller until the particular relational operator
		// has run to completion
		RelationalOp(){runlen =15000; pipe_size = 2000;}
	virtual void WaitUntilDone () = 0;
	// tells how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectPipe : public RelationalOp
{
private:
	pthread_t m_thread;
	int runlen;
	struct Relation
	{
		Pipe *iPipe;
		Pipe *oPipe;
		CNF *cnf;
		Record *litRec;

		Relation(Pipe *inPipe, Pipe *outPipe, CNF *selOp, Record *literal)
		{
			iPipe = inPipe;
			oPipe = outPipe;
			cnf = selOp;
			litRec = literal;
		}
	};
	static void* executeThread(void*);


public:

	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class SelectFile : public RelationalOp
{

public:
	pthread_t m_thread;
	int runlength;
	struct Relation
	{
		DBFile *iFile;
		Pipe *oPipe;
		CNF *cnf;
		Record *litRec;

		Relation(DBFile *inFile, Pipe *outPipe, CNF *selOp, Record *literal)
		{
			iFile = inFile;
			oPipe = outPipe;
			cnf = selOp;
			litRec = literal;
		}
	};
	static void* executeThread(void*);
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};

class Project : public RelationalOp
{

private:
	pthread_t m_thread;

	struct Relation
	{
		Pipe *iPipe, *oPipe;
		int numAttsNew, numAttsOrig;
		int * pAttsNew;

		Relation(Pipe *inPipe, Pipe *outPipe, int *keepMe,
				int numAttsInput, int numAttsOutput)
		{
			iPipe = inPipe;
			oPipe = outPipe;
			numAttsNew = numAttsOutput;
			numAttsOrig = numAttsInput;
			pAttsNew = keepMe;
		}
	};
	static void* executeThread(void*);

public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

/*

class Join : public RelationalOp {
private:
	pthread_t m_thread;
	int runlen;
	struct Relation
	{
		Pipe *inL;
		Pipe *inR;
		Pipe *out;
		CNF *select;
		Record *literal;

		Relation(Pipe *inPipeL, Pipe *inPipeR, Pipe *outPipe, CNF *selOp, Record *literal)
		{
			inL = inPipeL;
			inR = inPipeR;
			out = outPipe;
			select = selOp;
			//literal = literal;
		}
	};
	static void* executeThread(void*);
public:
	//Join(){};

	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	//void CompareleftAndVector(vector V);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};*/

class Join : public RelationalOp {
	pthread_t thread;
public:
			Pipe* inPipeL;
			Pipe* inPipeR;
			Pipe* outPipe;
			CNF* selOp;
			Record* literal;

			Join(){}
			Join(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal, int runlen);

			void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);// { }
			void WaitUntilDone ();// { }
			void Use_n_Pages (int n);// { }
};

class DuplicateRemoval : public RelationalOp
{
private:

	int i_runlen;
	pthread_t m_thread;
	struct Relation
	{
		Pipe *iPipe;
		Pipe *oPipe;
		Schema *iSchema;
		int runlen;

		Relation(Pipe *inPipe, Pipe *outPipe, Schema *mySchema, int i_runlen)
		{
			iPipe = inPipe;
			oPipe = outPipe;
			iSchema = mySchema;
			runlen = i_runlen;
		}
	};
	static void* executeThread(void*);

public:

	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};

class Sum : public RelationalOp
{

private:
	pthread_t m_thread;
	struct Relation
	{
		Pipe *iPipe, *oPipe;
		Function *Func;
		Relation(Pipe *inPipe, Pipe *outPipe, Function *computeMe)
		{
			iPipe = inPipe;
			oPipe = outPipe;
			Func = computeMe;
		}
	};
	static void* executeThread(void*);

public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);

};


/*class GroupBy : public RelationalOp {
private:
	int m_nRunLen;
	pthread_t m_thread;


	struct Relation
	{
		Pipe *oPipe;
		Pipe  *iPipe;
		OrderMaker *groupAttr;
		Function *Func;
		int runLen;

		Relation(Pipe *inPipe, Pipe *outPipe, OrderMaker *groupAtts, Function *computeMe, int runlen)
		{
			iPipe = inPipe;
			oPipe = outPipe;
			groupAttr = groupAtts;
			Func = computeMe;
			runLen = runlen;
		}
	};
	static void* executeThread(void*);

public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};*/
/*
class GroupBy : public RelationalOp {
private:
	pthread_t m_thread;
	int runLen;
	struct Relation
	{
		/*	Pipe *oPipe, *iPipe;
		OrderMaker *grpAttr;
		Function *Func;
		int runLen;

		Relation(Pipe *inPipe, Pipe *outPipe, OrderMaker *groupAtts, Function *computeMe, int runlen)
		{
			iPipe = inPipe;
			oPipe = outPipe;
			grpAttr = groupAtts;
			Func = computeMe;
			runLen = runlen;
		}
		Pipe *in;
		Pipe *out;
		OrderMaker *gAtts;
		Function *compute;
		int runLength;
		Relation(Pipe *inPipe, Pipe *outPipe, OrderMaker *groupAtts, Function *computeMe, int runlen)
		{
			in= inPipe;
			out = outPipe;
			gAtts = groupAtts;
			compute = computeMe;
			runLength = runlen;
		}
	};
	static void* executeThread(void*);

public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
}; */
class GroupBy : public RelationalOp {
	pthread_t thread;
	public:
            Pipe* inPipe;
            Pipe* outPipe;
            OrderMaker* groupAtts;
            Function* computeMe;


            GroupBy(){}
            GroupBy(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe)
            {
                this->inPipe = &inPipe;
                this->outPipe = &outPipe;
                this->groupAtts = &groupAtts;
                this->computeMe = &computeMe;
            }

            void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) ;///{ }
            void WaitUntilDone () {pthread_join(thread,NULL); }
            void Use_n_Pages (int n) {runlen = n; }


};
class WriteOut : public RelationalOp
{
private:
	pthread_t m_thread;
	struct Relation
	{
		Pipe *iPipe;
		Schema *iSchema;
		FILE *ifile;
		int *count;

		Relation(Pipe *inPipe, Schema *pMySchema, FILE *outFile, int *cnt = NULL)
		{
			iPipe = inPipe;
			iSchema = pMySchema;
			ifile = outFile;
			count = cnt;
		}
	};
	static void* executeThread(void*);

public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
#endif
