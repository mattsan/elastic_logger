require 'thor'
require 'elasticsearch'

class Storer
  def initialize(index, type)
    @index = index
    @type = type
    @es = Elasticsearch::Client.new
  end

  def bulk(lines)
    actions = lines.map {|line|
      {
        index: {
          _index: @index,
          _type: @type,
          data: {
            line: line
          }
        }
      }
    }
    @es.bulk(body: actions)
  end
end

class Logger < Thor
  class_option :index, aliases: '-i', decs: 'index', required: true
  class_option :type, aliases: '-t', decs: 'type', required: true

  desc :store, 'store logs to Elasticsearch'
  def store(*filenames)
    filenames.each do |filename|
      store_file(filename)
    end
  end

  desc :search, 'search log lines stored Elasticsearch'
  option :words, aliases: '-w', desc: 'target words', type: :array, required: true
  option :show, desc: 'show lines', type: :boolean
  def search
    es = Elasticsearch::Client.new
    phrases = options['words'].map {|word|
      {
        match_phrase: {
          line: word
        }
      }
    }
    body = {
      query: {
        bool: {
          must: phrases
        }
      }
    }
    result = es.search(index: index, type: type, scroll: '5m', body: body)
    shell.say("total #{result['hits']['total']} hits", :green)

    lines = []

    if options['show']
      until result['hits']['hits'].empty?
        hits = result['hits']['hits']
        hits.each do |hit|
          lines << hit['_source']['line']
        end
        result = es.scroll(scroll: '5m', body: {scroll_id: result['_scroll_id']})
      end
    end

    puts lines.sort
  end

  no_commands do
    def index; @index ||= options[:index]; end
    def type; @type ||= options[:type]; end

    def store_file(filename)
      shell.say("storing #{filename}", :green)

      lines = []
      line_count = 0
      step_count = 0

      File.open(filename) do |file|
        until file.eof?
          lines << file.readline
          line_count += 1
          if line_count >= 10000
            storer.bulk(lines)
            step_count += line_count
            shell.say("#{step_count} lines stored\r", :green, false)
            lines = []
            line_count = 0
          end
        end

        unless lines.empty?
          storer.bulk(lines)
          step_count += lines.size
        end
      end

      shell.say("#{step_count} lines stored", :green)
    end

    def storer
      @storer ||= Storer.new(index, type)
    end
  end
end

Logger.start
