//
// Created by meox on 27/08/16.
//

#ifndef PUBSUB_PUBSUB_HPP_H
#define PUBSUB_PUBSUB_HPP_H

#include <memory>
#include <vector>
#include <thread>
#include <map>
#include <mutex>
#include <atomic>

#include <boost/lockfree/queue.hpp>


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
        Publisher(const topic_ptr_t<T>& topic) : _topic{topic}
        {}

        void produce(const topic_ptr_t<T>& t, const T& msg) { t->send(msg); }

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
                sub->deliver(this, data);
        }

        void subscribe(Subscriber<T>* s)
        {
            subs.push_back(s);
        }

        size_t get_id() const
        {
            return id;
        }

    private:
        static size_t get_counter()
        {
            static std::size_t counter = 0;
            counter++;
            return counter;
        }

        size_t id{get_counter()};
        std::string _name{};
        std::vector<Subscriber<T>*> subs;
    };

    template <typename T>
    struct msg_container_t
    {
		const Topic<T>* topic_ptr;
		T data;
    };

    template <typename T>
    class Subscriber
    {
    public:
        using f_callback_t = std::function<void(const Topic<T>* topic, T data)>;
        using queue_t = boost::lockfree::queue<msg_container_t<T>>;

        using topic_raw_ptr = const Topic<T>*;
		using data_t = const T&;

        Subscriber() = default;
        Subscriber(f_callback_t f) : callaback{std::move(f)}
        {}

        void subscribe(const std::vector<topic_ptr_t<T>>& topics)
        {
            for (const auto& topic : topics)
                subscribe(topic);
        }

		void subscribe(const std::initializer_list<topic_ptr_t<T>>& topics)
		{
			for (const auto& topic : topics)
				subscribe(topic);
		}

        void subscribe(const std::vector<topic_ptr_t<T>>& topics, f_callback_t f)
        {
            callaback = std::move(f);
            for (const auto& topic : topics)
                subscribe(topic);
        }

        void subscribe(const topic_ptr_t<T>& topic)
        {
            //m_data[topic->get_id()] = std::make_shared<queue_t>(300000ull);
            topic->subscribe(this);
        }

        virtual void deliver(topic_raw_ptr topic, data_t e)
        {
            //auto& data = m_data.at(topic->get_id());
			msg_container_t<T> msg{topic, e};

			while (true)
            {
                bool b = data.push(msg);
                if (b)
                    break;
                else
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }

        void run()
        {
            stopped = false;
            th = std::thread([this](){
                event_loop();
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

        virtual ~Subscriber()
        {
            unsubscribe();
        }

    protected:
        virtual void event_loop()
        {
			msg_container_t<T> msg;

            while (true)
            {
                bool g_data{false};
				const auto is_data = data.pop(msg);
				if (is_data)
				{
					g_data = true;
					execute(msg.topic_ptr, msg.data);
				}

                if (!g_data && stopped)
                    break;

                if (!g_data) // no data at all (for every queue)
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
        }

        virtual void execute(topic_raw_ptr topic, data_t data)
        {
            if(callaback)
                callaback(topic, data);
        }

    private:
        f_callback_t callaback;
        queue_t data;
        std::atomic<bool> stopped{true};
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

	template <typename T, typename Q>
	publisher_ptr_t<T> create_publisher(topic_ptr_t<T> topic)
	{
		return std::make_shared<Q>(topic);
	}


	template <typename T>
    subscriber_ptr_t<T> create_subscriber(const topic_ptr_t<T>& topic, typename Subscriber<T>::f_callback_t&& f)
    {
        auto s = std::make_shared<Subscriber<T>>(f);
        s->subscribe(topic);
        return s;
    }

    template <typename T, typename F>
    subscriber_ptr_t<T> create_subscriber(const std::vector<topic_ptr_t<T>>& topics, F&& f)
    {
        auto s = std::make_shared<Subscriber<T>>(f);
        s->subscribe(topics);
        return s;
    }

    template <typename T, typename F>
    subscriber_ptr_t<T> create_subscriber(const std::initializer_list<topic_ptr_t<T>>& topics, F&& f)
    {
        std::vector<topic_ptr_t<T>> v_t{topics};
        return create_subscriber(v_t, std::forward<F>(f));
    }
}

#endif //PUBSUB_PUBSUB_HPP_H
