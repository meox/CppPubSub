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

template <typename T>
class Subscriber;

template <typename T>
class Topic;


template <typename T>
class Publisher
{
public:
    Publisher() = default;
    Publisher(Topic<T>* topic) : _topic{topic}
    {}

    void produce(Topic<T>& t, const T& msg)
    {
        t.send(msg);
    }

    void produce(const T& msg)
    {
        if(_topic != nullptr)
            _topic->send(msg);
    }

private:
    Topic<T>* _topic{nullptr};
};


template <typename T = std::string>
class Topic
{
public:
    using msg_type = T;

    Topic() = default;
    Topic(std::string name) : _name{std::move(name)}
    {}

    void send(const msg_type& data)
    {
        for(auto& sub : subs)
            sub->deliver(_name, data);
    }

    void subscribe(Subscriber<T>* s)
    {
        subs.push_back(s);
    }

    Publisher<T> create_publisher()
    {
        return Publisher<T>(this);
    }

private:
    std::string _name{};
    std::vector<Subscriber<T>*> subs;
};


template <typename T>
class Subscriber
{
public:
    using f_callback_t = std::function<void(const std::string& topic_name, T data)>;

    Subscriber(f_callback_t f) : callaback{std::move(f)}
    {}

    Subscriber(Topic<T>& topic, f_callback_t f) : callaback{std::move(f)}
    {
        subscribe(topic);
    }

    void subscribe(Topic<T>& topic)
    {
        topic.subscribe(this);
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

    void stop()
    {
        if (!stopped)
        {
            stopped = true;
            th.join();
        }
    }

    ~Subscriber()
    {
        stop();
    }

private:

    std::function<void(const std::string& topic_name, const T& data)> callaback;
    std::queue<std::pair<std::string, T>> data;
    std::atomic<bool> stopped{true};
    std::mutex m;
    std::thread th;
};

#endif //PUBSUB_PUBSUB_HPP_H
