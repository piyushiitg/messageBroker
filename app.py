import itertools
import Queue
import threading
import time
import random
max_sleep = 1
messages = ["Hello", "Hi", "How are you"]

class Publisher(threading.Thread):
    def __init__(self, name, msg_broker):
        threading.Thread.__init__(self)
        self.name = name
        self.msg_broker = msg_broker

    def run(self):
        while True:
            topic = self.msg_broker.get_topic()
            message = random.choice(messages)
            self.send(self.name, topic, message)
            time.sleep(max_sleep)

    def send(self, publisher_name, topic, message):
        #print "%s: %s" % (publisher_name, time.ctime(time.time()))
        self.msg_broker.send(publisher_name, topic, message)

class Subscriber:
    def __init__(self, name, topic):
        self.name = name
        self.topic = topic
    def receive(self, message):
        print('{} - {} got message "{}"'.format(self.name, self.topic, message))
        
class Topic:
    def __init__(self, topic_number):
        self.tid = topic_number
        self.topic_name = "Topic" + str(self.tid)
    
    def _get_topic_name(self):
        return self.topic_name

class MessageBroker:
    def __init__(self):
        self.subscribers = {}
        self.topic_queues = {}

    def create_topic_queues(self, no_of_topic):
        for i in range(no_of_topic):
            t1 = Topic(i)
            topic_name = t1._get_topic_name()
            self.topic_queues[topic_name] = Queue.Queue()

    def get_topic_names(self):
        return self.topic_queues.keys()

    def get_topic(self):
        return random.choice(self.topic_queues.keys())
       
    def send(self, publisher_name, topic, message):
        queue = self.topic_queues[topic]
        self.write_file(topic, message, publisher_name)
        queue.put(message)

    def write_file(self, topic, message, publisher):
        with open('data.txt', 'a') as the_file:
            the_file.write("%s: %s %s %s\n" % (time.ctime(time.time()), publisher, topic, message))

    def register(self, sub):
        if self.subscribers.has_key(sub.topic):
            self.subscribers[sub.topic].append(sub)
        else:
            self.subscribers[sub.topic] = [sub]

    def message_dispatcher(self):
        while True:
            for topic, queue in self.topic_queues.iteritems():
                if self.subscribers.has_key(topic):
                    self.dispatch(topic, queue)

    def dispatch(self, topic, queue):
        subscribers = self.subscribers[topic]
        while not queue.empty():
            message = queue.get()
            for subscriber in subscribers:
                subscriber.receive(message)


if __name__ == "__main__":
    topics = 3
    publisher = 3
    subscriber = 3
    
    #Step 1: Message Broker Initilized
    msgBroker = MessageBroker()
    msgBroker.create_topic_queues(topics)

    #Step 2: Create Publisher
    for p in range(publisher):
        pubThread = Publisher("Publisher-%s"%p, msgBroker)
        pubThread.start()

    #Step 3: Create Subscriber
    for s in range(subscriber):
        topic = msgBroker.get_topic()
        sub = Subscriber('Subscriber-%s'%s, topic)
        msgBroker.register(sub)

    # Step 4: Create Message Broker Worker Thread
    msgBroker.message_dispatcher()
