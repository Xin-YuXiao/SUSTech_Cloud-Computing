% 本函数用于将提取接收电流的幅值计算

% ReceiverAmp: Receiver函数得出的csv文件中的第三列归一化后的电压幅值
% MoniterAmp：使用Moniter函数得出的csv文件中的电流值，使用其首值符号判断最终电压值正负
% 输入一个名为ResultAmp的csv文件，其首行为最终计算得出的电压值，下面为每次激发记录到的归一化的电压值
function [Result,ResultAmp] = Result(ReceiverAmp,MoniterAmp)
    if rem(length(MoniterAmp),2)==1
        MoniterAmp = MoniterAmp(1:end-1);
    end
    ResultAmp = zeros(1, length(MoniterAmp)/2);
    num = 1;
    for i=1:2:length(MoniterAmp)
        ResultAmp(num) = ReceiverAmp(i)-ReceiverAmp(i+1); 
        num = num+1;
    end
    Result = mean(abs(ResultAmp));
    if ReceiverAmp(1)*MoniterAmp(1)<0
        Result = -Result;
    end
    % 转置矩阵以便按列存储
    combined_matrix = ResultAmp';
    
    % 定义文件名
    filename = 'ResultAmp.csv';
    
    % 打开文件
    fid = fopen(filename, 'w');
    
    % 写入注释行
    fprintf(fid, 'Result amp: %s\n', Result);
    
    % 逐行写入数据
    for i = 1:size(combined_matrix, 1)
        fprintf(fid, '%f\n', combined_matrix(i, :));
    end
    
    % 关闭文件
    fclose(fid);
    
    fprintf('ReceiverAmp successfully written to %s\n', filename);
end