import React from "react";
import { PieChart, Pie, Sector, Tooltip, type PieSectorShapeProps } from "recharts";

type PieChartProps = {
  data: any,
  colors: string[],
  width: number,
  height: number,
  textPositionX: number,
  textPositionY: number,
  textFillColor: string,
  textSize: number,
  text: string | number,
}

export const PieTypeChart = (props: PieChartProps) => {
  const { data, width, height, textPositionX, textPositionY, textFillColor, textSize, colors } = props

  const renderSector = (sectorProps: PieSectorShapeProps) => (
    <Sector {...sectorProps} fill={colors[sectorProps.index % colors.length]} />
  );

  return (
    <PieChart width={width} height={height}>
      <text x={textPositionX} y={textPositionY} textAnchor="middle" fill={textFillColor} fontSize={textSize}>
        {props.text}
      </text>
      <Pie
        data={data}
        innerRadius={70}
        outerRadius={80}
        dataKey="value"
        shape={renderSector}
      />
      <Tooltip />
    </PieChart>
  );
}
