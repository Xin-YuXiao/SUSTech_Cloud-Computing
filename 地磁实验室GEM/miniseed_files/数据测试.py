import obspy
import matplotlib.pyplot as plt
# 读取miniseed文件
# 假设你的文件名为'my_seismogram.seed'
seismogram = obspy.read('253000319.20240408.150400000.X.miniseed')

# 获取第一个通道的数据（通常地震波形有多个通道，这里假设只有一个）
data = seismogram[0].data

# 设置时间轴（假设采样率为100 Hz）
time = seismogram[0].times()  # 时间数组，单位是秒

# 绘制波形
plt.figure(figsize=(10, 6))
plt.plot(time, data)
plt.title('Seismic waveform')
plt.xlabel('Time (s)')
plt.ylabel('Amplitude')
plt.grid(True)
plt.show()