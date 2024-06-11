% 本函数用于提取接收电流的幅值和极点点位

% data: 接收端csv文件中读取的时间序列
% time: 接收端数据（也可直接使用对应的监测数据中的时间）
% Peak: 使用Moniter函数得出的csv文件中的极值索引点
% MoniterAmp：使用Moniter函数得出的csv文件中的电流值
function ReceiverAmp = Receiver(data,time,Peak,MoniterAmp)
    % 找到MoniterAmp中非0元素
    id = find(MoniterAmp ~= 0);
    MoniterAmp(id) = 1./MoniterAmp(id); % 按照与1a之间的比值运算，得到归一化因子，用于后续归一化
    ReceiverAmp = MoniterAmp;
    for i=1:length(Peak)
        if MoniterAmp~=0
            ReceiverAmp(i) = MoniterAmp(i)*mean(data(Peak(1,i):Peak(2,i)));
        else
            ReceiverAmp(i) = mean(data(Peak(1,i):Peak(2,i)));
        end     
    end

    % 合并向量
    combined_matrix = [Peak; ReceiverAmp];
    
    % 转置矩阵以便按列存储
    combined_matrix = combined_matrix';
    
    % 定义文件名
    filename = 'ReceiverData.csv';
    
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