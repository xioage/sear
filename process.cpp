#include <iostream>
#include <math.h>
#include <vector>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <fstream>
#include <algorithm>
#include <time.h>

#define TIME 77
#define SLOTS 6000
#define SESSIONS 4885
#define PEOPLE 2235
#define CACHESIZE 100
#define MAXTHREADS 25

#define UPTIME 5
#define DOWNTIME 5
#define LOCALTIME 100

/*
 * 1: 0.0167276
5: 0.0230444
9: 0.0198048
13: 0.0227342
17: 0.0227221
21: 0.028723
25: 0.0351694
50: 0.0516618
75: 0.0729074
100: 0.197228
125: 0.227336
150: 0.259223
175: 0.36999
200: 0.614497
225: 0.683961*/

/*1: 0.016754
5: 0.0161688
9: 0.0166452
13: 0.0161343
17: 0.0185968
21: 0.0191036
25: 0.0200433
29: 0.0236736
33: 0.0217142
37: 0.0254793
41: 0.0229777
66: 0.03218
91: 0.0374223
116: 0.0477759
141: 0.0498934
166: 0.0613052
191: 0.067237
216: 0.0696899
241: 0.103933*/



using namespace std;

struct session{
    int time;
    int id;
    int count;
    int *people;
};

struct quest{
    int person;
    int image;
    int repeats;
};

vector<struct session> sessions[TIME];
vector<struct quest> quests[TIME*SLOTS];
vector<int> cache[PEOPLE];
queue<int> queueNormal;
queue<int> queueHigh;
queue<int> queueTime;
int threads[MAXTHREADS] = { 0 };
int interval = 500;

int randomNum(int max) {
    return rand() % max;
}

void loadTraces() {
    ifstream file("dataset/mobicom06-trace.txt");
    string line, word;

    getline(file, line);
    int total = 0;
    for(int i = 0; i < SESSIONS; i++) {
    getline(file, line);
        istringstream is1(line);

        session curSession;

        is1>>word;
        curSession.time = stoi(word);
        is1 >> word;
        curSession.id = stoi(word);
        is1 >> word;
        curSession.count = stoi(word);

        total += curSession.count;
        curSession.people = (int*)malloc(sizeof(int)*curSession.count);

        getline(file, line);
        istringstream is2(line);
        for (int j = 0; j < curSession.count; j++) {
            is2 >> word;
            curSession.people[j] = stoi(word);
        }

        sessions[curSession.time].push_back(curSession);
    }
    cout << "total view count: " << total << endl;
    file.close();
}

void distributeQuests() {
    for (int t = 0; t < TIME; t++) {
        for (int i = 0; i < sessions[t].size(); i++) {
            session curSession = sessions[t][i];

            for (int j = 0; j < curSession.count; j++) {
                struct quest curQuest;
                curQuest.person = curSession.people[j] / 10;
                curQuest.image = curSession.id % 1000;

                int startSlot = randomNum(interval);
                for (int k = 0; k < SLOTS; k += interval) {
		    curQuest.repeats = k/interval;
                    quests[curSession.time * SLOTS + k + startSlot].push_back(curQuest);
                }
            }
        }
    }

#if 0
    for (int i = 0; i < TIME; i++) {
        for (int j = 0; j < SLOTS; j++) {
            cout << quests[i * SLOTS + j].size() << " ";
        }
        cout << endl;
    }
#endif
}

int curRunningTime(int runningThreads) {
    int time = 10 + runningThreads * 2;
    //cout<<"cur time "<<time<<endl;
    return time;
}

bool cacheHit(quest q) {
    for (int i = 0; i < cache[q.person].size(); i++) {
        if (cache[q.person][i] == q.image)
            return true;
    }
    return false;
}

void insertCache(int person, int image) {
    if(image == 0) return;

    //already exist
    for (int i = 0; i < cache[person].size(); i++) {
        if (cache[person][i] == image)
            return;
    }

    //cache size full, random replace
    if (cache[person].size() == CACHESIZE) {
        cache[person][randomNum(CACHESIZE)] = image;
        return;
    }

    cache[person].push_back(image);
}

int getCache(int person) {
    if (cache[person].size() == 0) return 0;
    int index = randomNum(cache[person].size());
    return cache[person][index];
}

void cacheSharing(int time) {
    for (int i = 0; i < sessions[time].size(); i++) {
        session curSession = sessions[time][i];

        random_shuffle(curSession.people, curSession.people + curSession.count);

        for (int j = 0; j < curSession.count - 1; j += 2) {
            insertCache(curSession.people[j]/10, getCache(curSession.people[j + 1]/10));
            insertCache(curSession.people[j + 1]/10, getCache(curSession.people[j]/10));
        }
    }
}


int main(int argc, char *argv[]) {
    int serverOnly = 0;
    double totalTime = 0;
    int totalQuests = 0;
    int latestTime = 0;
    int averageTime = 0;
    int totalCache = 0;
    int hittedCache = 0;
    int totalUp = 0, realUp = 0;

    if(argc < 3) {
	cout << "Usage: " << argv[0] << " interval[100/200/...] serverOnly[0/1]" << endl;
	return 1;
    } else {
	interval = stoi(argv[1]);
	serverOnly = stoi(argv[2]);
    }


    loadTraces();
    distributeQuests();

    for (int i = 0; i < TIME * SLOTS * 10; i++) {
        //schedule server execution
        int runningThreads = 0;
        for (int j = 0; j < MAXTHREADS; j++) {
            if(threads[j] > 0) threads[j]--;

            if (threads[j] == 0) {
                if (queueHigh.size() > 0) {
                    //queuing time
                    totalTime += i - queueHigh.front();
                    latestTime = i - queueHigh.front() + 20;
		    queueTime.push(latestTime);
                    queueHigh.pop();

                    threads[j] = -1;
                }
                else if (queueNormal.size() > 0) {
                    //queuing time
                    totalTime += i - queueNormal.front();
                    latestTime = i - queueNormal.front() + 20;
		    queueTime.push(latestTime);
                    queueNormal.pop();

                    threads[j] = -1;
                }
            }

            if (threads[j] != 0) runningThreads++;
        }

        for (int j = 0; j < MAXTHREADS; j++) {
            if (threads[j] == -1) {
                threads[j] = curRunningTime(runningThreads);
                //execution time
                totalTime += threads[j];
                totalQuests++;
            }
        }
        //latestTime += curRunningTime(runningThreads);

        //client cache sharing
        if(i % (100 * 10) == 0) cacheSharing(i / (SLOTS * 10));

        //client request generation
        if (i % 10 == 0) {
            vector<struct quest> curQuests = quests[i / 10];
            for (int j = 0; j < curQuests.size()/5; j++) {
                quest curQuest = curQuests[j];
                totalUp++;

                //edge process quest
		int server_est = 0;
                server_est = latestTime;
#if 0
		while(queueTime.size() > 5) queueTime.pop();
		for(int i = 0; i < queueTime.size(); i++) {
		    server_est += queueTime.front();
		    queueTime.push(queueTime.front());
		    queueTime.pop();
		}
		server_est = server_est * 1.0f / queueTime.size();
		server_est = (queueHigh.size() + queueNormal.size()) * 1.5f / MAXTHREADS * 50 + 20;
#endif

                if (serverOnly || server_est < LOCALTIME) {
                    //send quest to edge
                    totalTime += UPTIME;

                    //server process, time is determined later
                    queueNormal.push(i);

                    //server send result back
                    totalTime += DOWNTIME;

		    realUp++;
                }
                else {
                    //client local cache
                    totalTime += LOCALTIME;

		    //if(curQuest.repeats < 2) totalCache++;
		    totalCache++;
                    //local cache miss
                    if (!cacheHit(curQuest)) {
                        //send quest to edge
                        totalTime += UPTIME;

                        //server process, time is determined later
                        queueHigh.push(i);

                        //server send result back
                        totalTime += DOWNTIME;

			realUp++;
                    }
                    else {
                        totalQuests++;
			//if(curQuest.repeats < 2) hittedCache++;
			hittedCache++;
                    }
                }

                insertCache(curQuest.person, curQuest.image);
            }
        }

        //if (i != 0 && i % 100000 == 0) cout << totalTime * 1.0f / totalQuests << endl;
        if (i != 0 && i % 100000 == 0) cout << hittedCache * 1.0f / totalCache << endl;
        //if (i != 0 && i % 100000 == 0) cout << totalUp << endl;
    }

    return 0;
}
