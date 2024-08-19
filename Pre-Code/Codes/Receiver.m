% 本函数用于提取接收电流的幅值和极点点位

% data: 接收端csv文件中读取的时间序列
% time: 接收端数据（也可直接使用对应的监测数据中的时间）
% Peak: 使用Moniter函数得出的csv文件中的极值索引点
% MoniterAmp：使用Moniter函数得出的csv文件中的电流值
function ReceiverAmp = Receiver(data,Peak,MoniterAmp)
    ReceiverAmp = MoniterAmp;    
    for i=1:length(Peak)      
        ReceiverAmp(i) = mean(data(Peak(1,i):Peak(2,i)));   
    end
end