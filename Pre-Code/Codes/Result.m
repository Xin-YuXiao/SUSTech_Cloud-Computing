% 本函数用于将提取接收电流的幅值计算

% ReceiverAmp: Receiver函数得出的csv文件中的第三列归一化后的电压幅值
% MoniterAmp：使用Moniter函数得出的csv文件中的电流值，使用其首值符号判断最终电压值正负
% 输入一个名为ResultAmp的csv文件，其首行为最终计算得出的电压值，下面为每次激发记录到的归一化的电压值
function [Result,ResultAmp] = Result(ReceiverAmp,MoniterAmp)
    % 找到MoniterAmp中非0元素
    id = find(MoniterAmp ~= 0);
    MoniterAmp(id) = 1./MoniterAmp(id); % 按照与1a之间的比值运算，得到归一化因子，用于后续归一化
    
    if rem(length(MoniterAmp),2)==1
        MoniterAmp = MoniterAmp(1:end-1);
    end
    ResultAmp = zeros(1, length(MoniterAmp)/2);
    num = 1;
    for i=1:2:length(MoniterAmp)
        ResultAmp(num) = (ReceiverAmp(i)-ReceiverAmp(i+1)); 
        num = num+1;
    end
    MoniterAmp(MoniterAmp==0) = []; % 将数组中为0的值赋空，便于归一化乘算
    ResultAmp = ResultAmp .* abs(MoniterAmp);
    Result = mean(abs(ResultAmp));
    if ReceiverAmp(1)*MoniterAmp(1)<0
        Result = -Result;
    end
end