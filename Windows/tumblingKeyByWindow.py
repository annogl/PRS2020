# from datetime import datetime
# from typing import Collection, Iterable, Tuple
# from typing import TypeVar
#
# from pyflink.common import Types, TypeSerializer, Time
# from pyflink.datastream import StreamExecutionEnvironment, WindowAssigner, TimeWindow, ProcessWindowFunction
# from pyflink.datastream.window import TimeWindowSerializer, Trigger
# from pyflink.fn_execution.table.window_trigger import ProcessingTimeTrigger
# from pyflink.table import StreamTableEnvironment
#
# T = TypeVar("T")
#
#
# class TumblingProcessingTimeWindows(WindowAssigner[T, TimeWindow]):
#     """
#     A WindowAssigner that windows elements into windows based on the current system time of
#     the machine the operation is running on. Windows cannot overlap.
#
#     For example, in order to window into windows of 1 minute, every 10 seconds:
#
#     ::
#
#         >>> data_stream.key_by(lambda x: x[0], key_type=Types.STRING()) \\
#         ...     .window(TumblingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(10)))
#     """
#
#     def __init__(self, size: int, offset: int):
#         if abs(offset) >= size:
#             raise Exception("TumblingProcessingTimeWindows parameters must satisfy "
#                             "abs(offset) < size")
#
#         self._size = size
#         self._offset = offset
#
#     @staticmethod
#     def of(size: Time, offset: Time = None) -> 'TumblingProcessingTimeWindows':
#         """
#         Creates a new :class:`TumblingProcessingTimeWindows` :class:`WindowAssigner` that assigns
#         elements to time windows based on the element timestamp and offset.
#
#         For example, if you want window a stream by hour, but window begins at the 15th minutes of
#         each hour, you can use of(Time.hours(1), Time.minutes(15)), then you will get time
#         windows start at 0:15:00,1:15:00,2:15:00,etc.
#
#         Rather than that, if you are living in somewhere which is not using UTC±00:00 time, such as
#         China which is using UTC+08:00, and you want a time window with size of one day, and window
#         begins at every 00:00:00 of local time, you may use of(Time.days(1), Time.hours(-8)).
#         The parameter of offset is Time.hours(-8) since UTC+08:00 is 8 hours earlier than UTC time.
#
#         :param size The size of the generated windows.
#         :param offset The offset which window start would be shifted by.
#         :return The time policy.
#         """
#         if offset is None:
#             return TumblingProcessingTimeWindows(size.to_milliseconds(), 0)
#         else:
#             return TumblingProcessingTimeWindows(size.to_milliseconds(), offset.to_milliseconds())
#
#     def assign_windows(self,
#                        element: T,
#                        timestamp: int,
#                        context: WindowAssigner.WindowAssignerContext) -> Collection[TimeWindow]:
#         current_processing_time = context.get_current_processing_time()
#         start = TimeWindow.get_window_start_with_offset(current_processing_time, self._offset,
#                                                         self._size)
#         return [TimeWindow(start, start + self._size)]
#
#     def get_default_trigger(self, env) -> Trigger[T, TimeWindow]:
#         return ProcessingTimeTrigger()
#
#     def get_window_serializer(self) -> TypeSerializer[TimeWindow]:
#         return TimeWindowSerializer()
#
#     def is_event_time(self) -> bool:
#         return False
#
#     def __repr__(self):
#         return "TumblingProcessingTimeWindows(%s, %s)" % (self._size, self._offset)
#
# class MyProcessWindowFunction(ProcessWindowFunction):
#
#     def process(self, key: str, context: ProcessWindowFunction.Context,
#                 elements: Iterable[Tuple[float, int, str]]) -> Iterable[str]:
#         count = 0
#         for _ in elements:
#             count += 1
#         yield "Window: {} count: {}".format(context.window(), count)
#
#     def clear(self, context: 'ProcessWindowFunction.Context') -> None:
#         return
#
# def main():
#     env = StreamExecutionEnvironment.get_execution_environment()
#     t_env = StreamTableEnvironment.create(stream_execution_environment=env)
#     t_env.get_config().get_configuration().set_string("parallelism.default", "1")
#     t_env.get_config().get_configuration().set_string("pipeline.auto-watermark-interval", "800ms")
#
#     data_stream = env.from_collection(
#         collection=[(datetime.timestamp(datetime.now()), 1, 'aaa'), (datetime.timestamp(datetime.now()), 2, 'bb'), (datetime.timestamp(datetime.now()), 3, 'c')],
#         type_info=Types.ROW([Types.FLOAT(), Types.INT(), Types.STRING()]))
#
#     data_stream.key_by(lambda a: a[0]) \
#         .window(TumblingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(10))) \
#         .process(MyProcessWindowFunction()) \
#         .print()
#
#     env.execute()
#
#
# if __name__ == '__main__':
#     main()
