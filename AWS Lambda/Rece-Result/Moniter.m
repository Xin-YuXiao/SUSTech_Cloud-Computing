% 本函数用于提取监测电流的幅值和极点点位

% data: 从监测数据的csv文件中读取的时间序列
% time: csv文件开始记录的时间 eg：time = loadedData{1}.(2)(1)
% Hall_coefficient: 监测段记录电压值与电流值之间的转换系数
function [Peak,DC_amp] = Moniter(data,time,Hall_coefficient)

window_size = 20; % 窗口大小
num_windows = length(data) - window_size + 1; % 窗口数目

% 初始化结果数组
data_y = zeros(1, num_windows);

% 遍历每个窗口并计算均值的差值
for i = 1:num_windows
    idx = i:(i+window_size-1); % 当前窗口的滑动索引
    if i+window_size > num_windows
        idy = idx;
    else
        idy = (i+window_size):(i+2*window_size-1);
    end
    data_y(i) = (mean(data(idy))-mean(data(idx))); % 计算当前窗口的均值
end

absnum = max(data_y)/2;
% 将data数数组中小于absnum的数值赋值为0
idz = abs(data_y) < absnum;
data_y(idz) = 0;

% 找到所有极值z
peak_sort = zeros(1,length(data_y));
for i = 2:length(data_y)-1
    if (data_y(i)-data_y(i-1))*(data_y(i+1)-data_y(i)) < 0
        peak_sort(i) = i;
    end
end

peak_sort(peak_sort==0)=[]; % 将peak_sort中0点去掉，只保留极值点

% 由于背景场波动，提取极值点时会有临近点误识别为极值点
% 需要判断两点之间是否相隔过近
peak = zeros(1,length(peak_sort));
for i = 1:length(peak_sort)-1
    if peak_sort(i+1) - peak_sort(i) < 20
        peak(i) = peak_sort(i);
    end
end

% 找到两个数组中相同的元素
[~, id_peak] = ismember(peak_sort, peak);

% 删除相同的元素
peak_sort(id_peak~=0) = [];

% 根据提取出的极值点，调用RemoveOutliers函数，选择合适的范围
% 使得所选取用于计算幅值的数据点中不包含离群点
DC_amp = zeros(1,length(peak_sort)-1);
peakindices = zeros(2,length(peak_sort)-1);

for i = 1:length(peak_sort)-1
    [mean_value, indices] = RemoveOutliers(data(peak_sort(i):peak_sort(i+1)));
    DC_amp(i) = mean_value;

    peakindices(1,i) = peak_sort(i)+min(indices);
    peakindices(2,i) = peak_sort(i)+max(indices);
end

DC_amp(abs(DC_amp)<(max(DC_amp)/2))=0; % 将DC_amp中无激发时的电流幅值归为0

DC_amp = DC_amp.*Hall_coefficient;% 乘以转换系数，将电压转换为电流
Peak = peakindices;
% 合并向量
combined_matrix = [peakindices; DC_amp];

% 转置矩阵以便按列存储
combined_matrix = combined_matrix';

% 定义文件名
filename = 'MoniterData.csv';

% 打开文件
fid = fopen(filename, 'w');

% 提取年月日时分部分并格式化为 'YYYYMMDDHHMM' 形式
dateTimePart = datestr(time, 'yyyymmddHHMM');

% 提取秒部分
secondsPart = datestr(time, 'ss');

% 写入注释行
fprintf(fid, '%s,%s,0.001s\n', dateTimePart,secondsPart);

% 逐行写入数据
for i = 1:size(combined_matrix, 1)
    fprintf(fid, '%f, %f, %f\n', combined_matrix(i, :));
end

% 关闭文件
fclose(fid);

fprintf('Data successfully written to %s\n', filename);

end