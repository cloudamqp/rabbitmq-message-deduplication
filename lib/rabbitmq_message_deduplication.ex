# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2026, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication do

  use Application

  # Start a dummy supervisor to enable the Application behaviour.
  # https://erlang.org/pipermail/erlang-questions/2010-April/050508.html
  @impl true
  def start(_, _) do
    Supervisor.start_link(__MODULE__, [], [])
  end

  @impl true
  def stop(_) do
    RabbitMQMessageDeduplication.Exchange.unregister()
    RabbitMQMessageDeduplication.PolicyEvent.disable()
    restore_backing_queue()
  end

  # Restoring the backing queue changes the environment of the `rabbit`
  # application, a request served by the application controller. The controller
  # is busy stopping this application until this callback returns, so making
  # the request from here would block until it times out. It is made from a
  # separate process instead, and served as soon as the application stopped.
  #
  # Once the application stopped, its master kills every process the
  # application left behind, which is why the process is moved out of it.
  defp restore_backing_queue() do
    pid = spawn(&RabbitMQMessageDeduplication.Queue.disable/0)
    Process.group_leader(pid, Process.whereis(:init))

    :ok
  end

  def init([]) do
    Supervisor.init([], strategy: :one_for_one)
  end
end
