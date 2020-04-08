#include "common.h"
#include "BoundedBuffer.h"
#include "Histogram.h"
#include "common.h"
#include "HistogramCollection.h"
#include "FIFOreqchannel.h"
#include <thread>
using namespace std;
void closeChannel(FIFORequestChannel * channel);



double requestDatapoint(FIFORequestChannel* channel, datamsg* dm){   
    //datamsg * dm = (datamsg*)msgBuffer;
    int person = dm->person;
    double time = dm->seconds;
    int ecg = dm->ecgno;
    //cout << "->Requesting data point for Patient: " << person << ", Time " << time << ", ECG: " << ecg << endl; 
    channel->cwrite(dm,sizeof(datamsg));
    double response;
    channel->cread((char*)&response,sizeof(double));
    //cout << "-->DataPoint Request Completed" << endl;
    //cout << "--->Person: " << person << ", time: " << time << ", ecg" << ecg << ": " << response << endl << endl;
    return response;
}

void patient_thread_function(int nPoints, int patient, BoundedBuffer* requestBuffer){
    datamsg d(patient,0.0,1);
    cout << "-->Requesting " << nPoints << " datapoints from patient: " << patient << endl;
    for(int i = 0; i < nPoints;++i){
        requestBuffer->push((char *) &d, sizeof(datamsg));
        d.seconds += 0.004;
    }
    cout << "-->Datapoints Request Completed for Patient: " << patient << endl;
}


void file_thread_function(string fileName, BoundedBuffer* requestBuffer, FIFORequestChannel* mainChannel, int m){
    string rcvFileName = "received/" + fileName;
    char buf[1024];
    filemsg f(0,0);
    memcpy(buf, &f,sizeof(f));
    strcpy(buf+sizeof(f), fileName.c_str());
    int msgLength = sizeof(filemsg) + fileName.size() + 1;
    mainChannel->cwrite(buf,msgLength);
    __int64_t fileLength;
    mainChannel->cread(&fileLength,sizeof(fileLength));
    FILE* fp = fopen(rcvFileName.c_str(),"w");
    fseek(fp,fileLength,SEEK_SET);
    //GENERATING FILEMSGS
    filemsg* fm = (filemsg*)buf;
    __int64_t remlen = fileLength;
    cout << "-->Requesting File " << fileName << endl;
    while(remlen > 0){
        fm->length = min(remlen,(__int64_t)m);
        requestBuffer->push(buf,msgLength);
        fm->offset += fm->length;
        remlen -= fm->length;
    }
    cout << "-->Request for File: " << fileName << " Terminated" << endl;
}
void worker_thread_function(FIFORequestChannel* channel, BoundedBuffer* request_buffer, HistogramCollection* hc, int mb ){
    /*
		Functionality of the worker threads	
    */
   char msg_buffer[1024];
   double response = 0.0;
   char rcvBuffer[mb];
   while(true){
       request_buffer->pop(msg_buffer,1024);
       MESSAGE_TYPE* m = (MESSAGE_TYPE *) msg_buffer;
       if(*m == DATA_MSG){
           //request data point
           datamsg* dm = (datamsg *)msg_buffer;
           response = requestDatapoint(channel,dm);
           hc->update(dm->person,response);
       }else if(*m == FILE_MSG){
           filemsg* fm = (filemsg *)msg_buffer;
           string fileName = (char * )(fm +1);
           int sz = sizeof(filemsg) + fileName.length() + 1;
           channel->cwrite(msg_buffer,sz);
           channel->cread(rcvBuffer, mb);

           //writting to file from received buffer
           string rcvFileName = "received/" + fileName;
           FILE* fp = fopen(rcvFileName.c_str(),"r+");
           fseek(fp, fm->offset,SEEK_SET);
           fwrite(rcvBuffer,1, fm->length, fp);
           fclose(fp);
           //request file
       }else if(*m == QUIT_MSG){
           closeChannel(channel);
           break;
       }
   }
}


double calculateTime(timeval start,timeval end ){
    return ((end.tv_sec + end.tv_usec*1e-6) - (start.tv_sec + start.tv_usec*1e-6));
}

//populating histogram Collection
void creatHistograms(HistogramCollection &hc, int p){
    for(int i = 0; i < p; ++i){
        //Histogram *h = new Histogram(10,-2.0,2.0);
        hc.add(new Histogram(10,-2.0,2.0));
    }
}


//requesting new channel from main channel
FIFORequestChannel* requestNewChannel(FIFORequestChannel *channel){
    string chanelName;
    char* buffer = (char *)malloc(1024);
    MESSAGE_TYPE m = NEWCHANNEL_MSG;
    //cout << "->Requesting New Channel" << endl;
    channel->cwrite(&m,sizeof(MESSAGE_TYPE));
    channel->cread(buffer, 1024);
    chanelName = string(buffer);
    //cout << "-->Channel Request Completed, New Channel Name: " << chanelName << endl << endl;
    //ofstream out("received/newChannel.out");
    //out << chanelName << endl;
    //out.close();
    FIFORequestChannel* channel2 = new FIFORequestChannel(chanelName,FIFORequestChannel::CLIENT_SIDE);;
    return channel2;
}

void closeChannel(FIFORequestChannel * channel){
    //cout << "->Terminating Main Chanel: " << channel->name() << endl;   
    MESSAGE_TYPE m = QUIT_MSG;
    channel->cwrite(&m, sizeof (MESSAGE_TYPE));
    delete channel;
}
FIFORequestChannel** RequestChannels(int channelNum, FIFORequestChannel * mainChannel){
    FIFORequestChannel** channels = new FIFORequestChannel*[channelNum];
    for(int i = 0; i < channelNum; ++i){
        channels[i] =  requestNewChannel(mainChannel);
    }
    return channels;
}

int main(int argc, char *argv[])
{
    int n = 100;    //default number of requests per "patient"
    int p = 10;     // number of patients [1,15]
    int w = 100;    //default number of worker threads
    int b = 20; 	// default capacity of the request buffer, you should change this default
    string fileName = "";
	int m = MAX_MESSAGE; 	// default capacity of the message buffer
    srand(time_t(NULL));
    bool pflag = false; 
    bool fflag= false;
    int comm;
    while((comm = getopt(argc, argv, "n:p:w:b:f:m")) != -1){
        switch (comm)
        {
            case 'n':
                n = (int)atoi(optarg);
                break;
            case 'p':
                pflag = true;
                p = (int)atoi(optarg);
                break;
            case 'm':
                m = (int)atoi (optarg);
                break;
            case 'f':
                fflag = true;
                fileName.assign(optarg);
                break;
            case 'w':
                w = (int)atoi(optarg);
                break;
            case 'b':
                b = (int)atoi(optarg);
                break;
            default:
                break;
        }
    }
    cout << "Bounded Buffer Capacity: " << b << endl;
    cout << "Server Buffer Capacity:  " << m << endl;
    cout << "Data Points:             " << n << endl;
    cout << "Patients:                " << p << endl;
    cout << "Worker Threads:          " << w << endl;
    cout << "File Name:               " << fileName << endl; 
    int pid = fork();
    if (pid == 0){
		// modify this to pass along m
        execl ("server", "server", "-m", to_string(m).c_str(), (char *)NULL);
    }

    FIFORequestChannel* chan = new FIFORequestChannel("control", FIFORequestChannel::CLIENT_SIDE);
    BoundedBuffer request_buffer(b);
    HistogramCollection hc;

    //creating histogram
    creatHistograms(hc,p);

    //creating working channels
    FIFORequestChannel** wChannels = RequestChannels(w, chan);

    //creating worker threads
    thread workers[w];
    for(int i = 0; i < w; ++i){
        workers[i] = thread(worker_thread_function,wChannels[i], &request_buffer, &hc,m);
    }
    //handling person request
    if(pflag){
        struct timeval start, end;
        gettimeofday (&start, 0);

        /* Start all threads here */
        thread patients[p];
        for(int i =0; i<p; ++i){
            patients[i] = thread(patient_thread_function, n,i+1, &request_buffer);
        }


        /*End of threads*/

        cout << "Joining Threads" << endl;
        /* Join all threads here */
        for(int i = 0; i < p;++i){
            patients[i].join();
        }
        cout << "Joining Patien Threads Completed" << endl;

        cout << "Joining Worker Threads" << endl;
        //quitting workers
        for(int i = 0 ; i < w; ++i){
            MESSAGE_TYPE m = QUIT_MSG;
            request_buffer.push((char*)&m, sizeof(m));
        }
        
        cout << "Worker Threads Terminated" << endl;
        for(int i = 0; i < w;++i){
            workers[i].join();
        }
        cout << "Join completed" << endl;
        /*End of joining threads*/
        gettimeofday (&end, 0);
        // print the results
        hc.print ();
        int secs = (end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)/(int) 1e6;
        int usecs = (int)(end.tv_sec * 1e6 + end.tv_usec - start.tv_sec * 1e6 - start.tv_usec)%((int) 1e6);
        cout << "Took " << secs << " seconds and " << usecs << " micro seconds" << endl;

    }
    //handling file request
    else if(fflag){
        thread fileThread = thread(file_thread_function,fileName,&request_buffer,chan,m);

        fileThread.join();
        cout << "Joining Worker Threads" << endl;
        //quitting workers

        for(int i = 0 ; i < w; ++i){
            MESSAGE_TYPE m = QUIT_MSG;
            request_buffer.push((char*)&m, sizeof(m));
        }
        
        cout << "Worker Threads Terminated" << endl;
        for(int i = 0; i < w;++i){
            workers[i].join();
        }
        cout << "Join completed" << endl;

    }
    //Closing Main Channel
    closeChannel(chan);   
}
