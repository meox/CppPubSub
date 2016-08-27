//
// Created by meox on 27/08/16.
//

#ifndef PUBSUB_PUBSUB_HPP_H
#define PUBSUB_PUBSUB_HPP_H

#include <memory>
#include <vector>
#include <thread>
#include <queue>
#include <mutex>
#include <atomic>


namespace ps
{
    template <typename T>
    class Subscriber;

    template <typename T>
    class Topic;

    template <typename T>
    using topic_ptr_t = std::shared_ptr<Topic<T>>;

    template <typename T>
    using subscriber_ptr_t = std::shared_ptr<Subscriber<T>>;


    template <typename T>
    class Publisher
    {
    public:
        Publisher() = default;
        Publisher(topic_ptr_t<T> topic) : _topic{topic}
        {}

        void produce(topic_ptr_t<T> t, const T& msg)
        {
            t->send(msg);
        }

        void produce(const T& msg)
        {
            if(_topic)
                _topic->send(msg);
        }

    private:
        topic_ptr_t<T> _topic;
    };

    template <typename T>
    using publisher_ptr_t = std::shared_ptr<Publisher<T>>;


    template <typename T>
    class Topic
    {
    public:
        Topic() = default;
        Topic(std::string name) : _name{std::move(name)}
        {}

        void send(const T& data)
        {
            for(auto& sub : subs)
                sub->deliver(_name, data);
        }

        void subscribe(Subscriber<T>* s)
        {
            subs.push_back(s);
        }

    private:
        std::string _name{};
        std::vector<Subscriber<T>*> subs;
    };


    template <typename T>
    class Subscriber
    {
    public:
        using f_callback_t = std::function<void(const Topic<T>& topic, T data)>;

        Subscriber(f_callback_t f) : callaback{std::move(f)}
        {}

        Subscriber(topic_ptr_t<T> topic, f_callback_t f) : callaback{std::move(f)}
        {
            subscribe(topic);
        }

        Subscriber(const std::initializer_list<topic_ptr_t<T>>& topics, f_callback_t f) : callaback{std::move(f)}
        {
            for (auto& topic : topics)
                subscribe(topic);
        }

        void subscribe(topic_ptr_t<T> topic)
        {
            topic->subscribe(this);
        }

        void deliver(const std::string& topic_name, const T& msg)
        {
            std::lock_guard<std::mutex> l(m);
            data.push(std::make_pair(topic_name, msg));
        }

        void run()
        {
            stopped = false;
            th = std::thread([this](){
                while (true)
                {
                    std::unique_lock<std::mutex> g(m);
                    const auto is_empty = data.empty();
                    if (is_empty && stopped)
                        break;

                    if (!is_empty)
                    {
                        auto msg = data.front();
                        data.pop();
                        g.unlock();
                        callaback(msg.first, msg.second);
                    }
                    else
                    {
                        g.unlock();
                        std::this_thread::sleep_for(std::chrono::milliseconds(3));
                    }
                }
            });
        }

        void unsubscribe()
        {
            if (!stopped)
            {
                stopped = true;
                th.join();
            }
        }

        ~Subscriber()
        {
            unsubscribe();
        }

    private:

        f_callback_t callaback;
        std::queue<std::pair<std::string, T>> data;
        std::atomic<bool> stopped{true};
        std::mutex m;
        std::thread th;
    };


    template <typename T>
    topic_ptr_t<T> create_topic(const std::string& name)
    {
        return std::make_shared<Topic<T>>(name);
    }

    template <typename T>
    publisher_ptr_t<T> create_publisher(topic_ptr_t<T> topic)
    {
        return std::make_shared<Publisher<T>>(topic);
    }
}

#endif //PUBSUB_PUBSUB_HPP_H
