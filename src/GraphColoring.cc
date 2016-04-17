/*2,201518013229010,dongxiao*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) GraphColoring##name


class VERTEX_CLASS_NAME(InputFormatter):public InputFormatter{
public:
	int64_t getVertexNum(){
		unsigned long long n;
		sscanf(m_ptotal_vertex_line,"%lld",&n);
		m_total_vertex=n;
		return m_total_vertex;
	}
	int64_t getEdgeNum(){
		unsigned long long n;
		sscanf(m_ptotal_edge_line,"%lld",&n);
		m_total_edge=n;
		return m_total_edge;
	}
	int getVertexValueSize(){
		m_n_value_size = sizeof(int);
		return m_n_value_size;
	}
	//if no edge value?
	int getEdgeValueSize(){
		m_e_value_size= sizeof(int);
		return m_e_value_size;
	}
	int getMessageValueSize(){
		m_m_value_size= sizeof(int);
		return m_m_value_size;
	}
	void loadGraph(){
		unsigned long long last_vertex;
		unsigned long long from;
		unsigned long long to;

		int weight = 0;
		
		int value=-1;

		const char*line=getEdgeLine();
		sscanf (line,"%lld %lld %d",&from,&to,&weight);
		addEdge(from,to,&weight);

		last_vertex = from;
		int outdegree=0;
		outdegree++;
		for(int64_t i=1;i<m_total_edge;++i){
			line=getEdgeLine();

			sscanf (line,"%lld %lld %d",&from,&to,&weight);
			if(last_vertex!=from){
				addVertex(last_vertex,&value,outdegree);
				last_vertex=from;
				outdegree=1;
			}else{
				outdegree++;
			}
			addEdge(from,to,&weight);
		}
		addVertex(last_vertex,&value,outdegree);

	}
};

class VERTEX_CLASS_NAME(OutputFormatter):public OutputFormatter{
public:
	void writeResult(){
		int64_t vid;
		int value;
		const int max_line_length=1024;
		char s[max_line_length];

		for(ResultIterator r_iter;!r_iter.done();r_iter.next()){
			r_iter.getIdValue(vid,&value);
			int n=sprintf(s,"%lld:%d\n",(unsigned long long)vid,value);
			writeNextResLine(s,n);
		}
	}
};

//only global value is used
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<int> {
public:
    void init() {
        //printf("init aggregator!\n");
    }
    void* getGlobal() {
        return &m_global;
    }
    void setGlobal(const void* p) {
        m_global = *(int*)p;
    }
    void* getLocal() {
        return &m_local;
    }
    void merge(const void* p) {
        //printf("merge aggregator!\n");
    }
    void accumulate(const void* p) {
        //printf("accumulate aggregator!\n");
    }
};

class VERTEX_CLASS_NAME(): public Vertex <int, int, int> {
public:
    void compute(MessageIterator* pmsgs) {
        unsigned long long vertexId = getVertexId();
        int colorNumber=*(int*)getAggrGlobal(1);
        if (getSuperstep() == 0 && vertexId == *(unsigned long long*)getAggrGlobal(0)) {
           * mutableValue() = 0;
           sendMessageToAllNeighbors(0);
        } 
        else if(getSuperstep()>0){
        	//get colors from neibor
        	int cmpColor=getValue();
        	bool conflict=false;
            for ( ; ! pmsgs->done(); pmsgs->next() ) {
            	//conflicts with one color
            	if(cmpColor == pmsgs->getValue()){
                    srand((unsigned)time(NULL)*vertexId);
            		cmpColor=rand()%colorNumber;
            		conflict=true;
            	}
            }
            if(conflict){
            	* mutableValue() = cmpColor;
            	sendMessageToAllNeighbors(cmpColor);
            }  	
            else if(getValue()==-1){
                srand((unsigned)time(NULL)*vertexId);
                int newColor=rand()%colorNumber;
                * mutableValue() = newColor;
                sendMessageToAllNeighbors(newColor);
            }
            else{              
                voteToHalt(); 
            }
        }
    }
};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator)* aggs;
public:
    // argv[0]: GrapgCloring.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    // argv[3]: <v0>
    // argv[4]: <num_color>
    void init(int argc, char* argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 4111);
        setHost(1, "localhost", 4121);
        setHost(2, "localhost", 4131);
        setHost(3, "localhost", 4141);
        setHost(4, "localhost", 4151);
        //count from xxx.so
        if (argc < 5 ) {
           printf ("Usage: %s <input path> <output path> <v0> <num color>\n", argv[0]);
           exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];
        
        unsigned long long v0_id;
        int color_number;
        sscanf(argv[3],"%lld",&v0_id);
        sscanf(argv[4],"%d",&color_number);

        aggs = new VERTEX_CLASS_NAME(Aggregator)[2];

        regNumAggr(2);
        regAggr(0, &aggs[0]);
        regAggr(1, &aggs[1]);

        aggs[0].setGlobal(&v0_id);
        aggs[1].setGlobal(&color_number);
    }

    void term() {
        delete[] aggs;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}
