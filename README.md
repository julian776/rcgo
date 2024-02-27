# reactive-commons-go

The purpose of reactive-commons is to provide a set of abstractions and implementations over different patterns and practices that make the foundation of a reactive microservices architecture.

Even though the main purpose is to provide such abstractions in a mostly generic way such abstractions would be of little use without a concrete implementation so we provide some implementations in a best effors maner that aim to be easy to change, personalize and extend.

# Installation

`go get github.com/julian776/rcgo`

# [API GO DOCS](https://pkg.go.dev/github.com/julian776/rcgo)

# Examples

- [Publisher](./examples/e2e/publisher.go)
- [Listener](./examples/e2e/listener.go)

# Overview

Reactive Commons Go provides two primary implementations.

The Listener and the Publisher.

These components collaborate to enable asynchronous communication between services using RabbitMQ.

In Reactive Commons Go, the three fundamental concepts are commands, queries, and events.

## Commands

Commands in reactive-commons act as executors specific to an app and must be created with the app name followed by a specific identifier. For instance, in a payments app, a command could be named `payments.contacts.saveOrder`.

## Events

An event in reactive-commons represents a fact or occurrence. When you publish an event, it is broadcasted to all other Reactive Commons apps that have registered a handler for that specific event. Events don't have a specific target; instead, they are sent to all applications interested in them.

## Queries

In Reactive Commons, a query is a request for a specific resource to a specific target, prompting an asynchronous response from the app that possesses the resource. This interaction follows the Remote Procedure Call (RPC) pattern.

Read about RPC [here](https://www.rabbitmq.com/tutorials/tutorial-six-go.html)

To create a query in Reactive Commons, use the app name followed by a custom identifier. For instance, in a payments application, a query handler could be named `payments.contacts.getUser`.

# Test

`go test -v --cover $(go list ./... | grep -v /examples/)`

# How to Contribute

Make a pull request...

# License

Distributed under MIT License, please see license file within the code for more details.
