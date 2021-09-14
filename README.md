# Channel Pipeline Stream


In order to increase throughput of batch processes, channels can be used to allow multiple goroutines to work in different steps. 
For example: A data pipeline where a group of goroutines fetch information from the database and delivers it to another group, 
which are responsible for performing some business logic and forward to another group which saves the result back to the database.

Doing so increases the efficiency of the infrastructure, as we can process a much bigger amount of data in lesser time.

Pipeline pattern is an assembly line where partial results are passed from one stage to another. 
In our case, we will use a stream of channels that are being processed in each stage, traversing a PipelineData struct through a stream of channels.

## High Level Design

In order to achieve increased performance, use the FanIn/FanOut technique: spawn multiple goroutines for a given step, and then unite them back into a single channel.

Example, we could have a generator that provides a stream of data from an input (example, from a batcher file using patterns.Each or a custom reader if it's a Visa TC file),  and spawn a group of goroutines (a fixed number or runtime.NumCPU() to keep it reasonable) to process each step of the Pipeline.

Fig1: draw.io diagram of FanIn / Fanout

There are some simple principles that we followed to ensure that we introduced this approach without introducing bugs / keeping the code simple.

### 1 - Always close what you open:

Channels (as any resource) should always be closed by whoever created them - This ensures that our code will be less complicated and we won't have to worry about closing channels everywhere. 
A simple example of how to handle this, in a int generator stream:

```go
    func generateInts(ctx context.Context) <- chan interface{}{
        stream := make(chan interface{})
        go func() {
            defer close(stream) //this will execute after the spawned goroutine ends
            for {
                select{
                case <-ctx.Done(): // if context is cancelled, will leave
                    return
                case stream <- rand.Intn(500000000): //put a int into the stream
                }
            }
        }()

        return stream //defer won't execute after this return, as it belongs to the goroutine
    }
```

### 2 - NEVER use context.Background():

Using context.Background ties your goroutine to the execution of your main program. This can lead to deadlocks, as in the scenario where a goroutine hangs, it will be listening to the done of the context of the main goroutine. It's advisable to setup a timeout to whatever context will be passed to the pipeline, it doesn't need to be a small value - it just need to exist in order to free deadlocked resources, in the unfortunate scenario of this happening.

There are techniques to avoid the creation of deadlocks (see #1), but it's also importante to note that the risk exist and we need to design our application to be resilient, handling this scenario gracefully.

Using a context.WithTimeout is one of the ways to guarantee this. You may provide a context with timeout for the whole process, or spawn a child context (that will be cancelled if the father context is) for each entry with a smaller timeout.

### 3 - Avoid "super steps":

Having a super step brings the same problems of having a super class: 
It does everything, has tons of responsabilities and you'll have to scroll your mouse several times in order to (try to) understand the code.

This already smells in a normal, not concurrent scenario... Following the Single Responsability Principle pays off in making our lives easier, and it's specially true in a pipeline where things will be running concurrently.       

The ideal is that the steps do little transformations on the data, or a update to the database. If you see yourself validating an input, checking for deadlines, saving in the database and publishing an event in the same pipeline step, it's advised to do some refactoring.

### 4 - Failures will happen - be ready for them:

It can be a faulty connection, bad input file or a single register in a 100k entries file - things go bad, it's not a matter of if - it's a when.

We strongly suggest to design all batch proccess to be idempotent, retryable and fault tolerant. Also, avoid cancelling the whole batch because one line is faulty:

In a normal, linear, scenario this would be very previsible: 
    1...4 Successful
    5 failed
    5+ we don't know  

When we use FanOut, this would lead to the scenario where we have a faulty register but entries after it on the file could be already be processed (specially if the context is cancelled when a single failure is detected).
     1,2,3,4,6,7,8,9 Succesful
     5 failed 

In order to survive this, it is strongly advised to process the whole file and output whatever fails to an error file, which can be used later as input to another workflow / error handling step. Also, we may add a check of amount of errors, as it may reach a point where it doesn't make sense to continue processing (ex: database is down).

### 5 - Always have a good coverage of tests

This is a universal rule, but in code where bugs can be potentially destructive (ex: create deadlocks) test coverage should lie in the proximity of 90-95%. Always have tests that run under the interfaces you provide, and validate that your pipeline is working properly as a whole. If any piece of code is to be refactored to this pattern, it is strongly advised to increase test coverage of the sequential process before starting to implement a data Pipeline.

Always test the scenario of cancelled contexts, each step timing out (use a custom mocked struct that hangs forever) and integrations not working!

### 6 - Avoid using memory to comunicate between goroutines

Using memory to communicate between goroutines is a common pitfall that can lead to bugs. We prefer to use channels to communicate between our goroutines, which is a safer approach.
If the need arises (for example, for holding a cache of processed ids to avoid duplications), always use the sync package to control the access to these variables.

# Reusable functions

It is important to notice that in order to make these functions reusable, we declared the type of the channels as chan PipelineData. The "value" field is an interface that can be used to travel data between steps, we kept it as a interface to allow reuse.

While this removes the constraint of making all parts of the code (and potentially other repositories) having to use the same strong typed struct, it also leaves on the hand of the developer to use the same type in a given Pipeline.
Failing to do so will lead to hard to maintain code, which is not our goal!

"With great power, comes great resposability"

# OrDone

In order to hide some of the complexity of working with channels, we made some reusable functions to ensure that we won't have deadlocks. Using the OrDone func ensures that the goroutine will exit as soon as the context in cancelled, which is very important because we don't want goroutines leaks.  

Basically this function will encapsulate the logic of listening to two events that are of maximum importance when working with channels:
- Context is cancelled
- The inputStream that we are reading from is closed and no more elements are available

Using the OrDone func helps checking this, while keeping the code clean:

Fig2: Screenshot of OrDone code

Fig3: Screenshot of code using OrDone

# Generator

Generator will send input data to a channel, in order to provide the input as a stream. It's usually the first step of a pipeline, the one that will provide input to other steps.

# FanOut

FanOut will generate a pipeline step that spawns N goroutines using a streamFunc to create the channels. 

It is important to note that order must NOT be important to use FanIn/FanOut, all entries must be independent of each other (ex: entry 4 can not influence in any way entry 5, as they WILL be runned concurrently and in random order).

If order IS important, FanOut should NOT be used - although using a pipeline stream can still be used with one goroutine for each step, as order will be maintained: the input stream will make the data available in the order it receives. 

The gain from doing so is noticeable (specially if the steps are designed correctly), but it's lesser than a fanned out pipeline. 


Fig4: Screenshot of FanOut code

# FanIn

FanIn will read from a variadic slice of channels and join them into a single output channel. It will use a waitgroup to ensure that all channels were read from, and close the multiplexedStream afterwards.

Fig5: FanIn code
